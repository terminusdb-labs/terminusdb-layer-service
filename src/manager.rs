use std::{
    collections::HashSet,
    error::Error,
    io::{self, ErrorKind, SeekFrom},
    ops::Range,
    os::unix::prelude::MetadataExt,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_tempfile::TempFile;
use bytes::Bytes;
use futures::Stream;
use terminus_store::storage::{archive::ArchiveHeader, consts::LayerFileEnum, name_to_string};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::Mutex,
};
use tokio_stream::StreamExt;
use tokio_util::io::ReaderStream;

pub struct LayerManager {
    primary_path: PathBuf,
    local_path: PathBuf,
    upload_path: PathBuf,
    scratch_path: PathBuf,
    work_set: Mutex<HashSet<[u32; 5]>>,
}

impl LayerManager {
    pub fn new<P1: Into<PathBuf>, P2: Into<PathBuf>, P3: Into<PathBuf>, P4: Into<PathBuf>>(
        primary_path: P1,
        local_path: P2,
        upload_path: P3,
        scratch_path: P4,
    ) -> Self {
        LayerManager {
            primary_path: primary_path.into(),
            local_path: local_path.into(),
            upload_path: upload_path.into(),
            scratch_path: scratch_path.into(),
            work_set: Mutex::new(HashSet::new()),
        }
    }

    async fn file_reader(&self, path: &PathBuf) -> std::io::Result<Option<(usize, File)>> {
        let size = match tokio::fs::metadata(&path).await {
            Ok(m) => m.size() as usize,
            Err(e) => match e.kind() {
                ErrorKind::NotFound => return Ok(None),
                _ => return Err(e),
            },
        };

        let mut options = tokio::fs::OpenOptions::new();
        options.create(false);
        options.read(true);

        match options.open(&path).await {
            Ok(r) => Ok(Some((size, r))),
            Err(e) => match e.kind() {
                ErrorKind::NotFound => Ok(None),
                _ => Err(e),
            },
        }
    }

    fn primary_layer_file_path(&self, layer: [u32; 5]) -> PathBuf {
        let mut path = self.primary_path.clone();
        let name = name_to_string(layer);
        path.push(&name[0..3]);
        path.push(format!("{name}.larch"));

        path
    }

    async fn primary_layer_file_reader(
        &self,
        layer: [u32; 5],
    ) -> std::io::Result<Option<(usize, File)>> {
        let path = self.primary_layer_file_path(layer);
        self.file_reader(&path).await
    }

    fn local_layer_file_path(&self, layer: [u32; 5]) -> PathBuf {
        let mut path = self.local_path.clone();
        let name = name_to_string(layer);
        path.push(&name[0..3]);
        path.push(format!("{name}.larch"));

        path
    }

    async fn local_layer_file_exists(&self, layer: [u32; 5]) -> std::io::Result<bool> {
        let path = self.local_layer_file_path(layer);
        tokio::fs::try_exists(path).await
    }

    async fn local_layer_file_reader(
        &self,
        layer: [u32; 5],
    ) -> std::io::Result<Option<(usize, File)>> {
        let path = self.local_layer_file_path(layer);

        self.file_reader(&path).await
    }

    fn scratch_layer_file_path(&self, layer: [u32; 5]) -> PathBuf {
        let mut path = self.scratch_path.clone();
        let name = name_to_string(layer);
        path.push(format!("{name}.larch"));

        path
    }

    pub async fn get_layer_reader(
        self: Arc<Self>,
        layer: [u32; 5],
    ) -> std::io::Result<Option<(usize, File)>> {
        if let Some((size, reader)) = self.local_layer_file_reader(layer).await? {
            Ok(Some((size, reader)))
        } else if let Some((size, reader)) = self.primary_layer_file_reader(layer).await? {
            // attempt to cache this file
            self.clone().spawn_cache_layer(layer).await;
            tokio::spawn(try_copy_layer(self.clone(), layer));
            Ok(Some((size, reader)))
        } else {
            Ok(None)
        }
    }

    pub async fn get_layer(
        self: Arc<Self>,
        layer: [u32; 5],
    ) -> std::io::Result<Option<(usize, impl Stream<Item = io::Result<Bytes>> + Send)>> {
        self.get_layer_reader(layer)
            .await
            .map(|result| result.map(|(size, reader)| (size, ReaderStream::new(reader))))
    }

    pub async fn upload_layer(
        self: Arc<Self>,
        layer: [u32; 5],
        mut stream: impl Stream<Item = Result<Bytes, hyper::Error>> + Unpin,
    ) -> Result<(), Box<dyn Error>> {
        let mut file = TempFile::new_in(&self.upload_path).await?;
        while let Some(mut bytes) = stream.try_next().await? {
            file.write_all_buf(&mut bytes).await?;
        }
        file.flush().await?;

        self.move_uploaded_layer(layer, file.file_path()).await?;

        Ok(())
    }

    async fn move_uploaded_layer(
        self: Arc<Self>,
        layer: [u32; 5],
        file_path: impl AsRef<Path>,
    ) -> io::Result<()> {
        let destination_path = self.primary_layer_file_path(layer);
        if let Some(parent) = destination_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        eprintln!("{:?}", file_path.as_ref());
        tokio::fs::rename(file_path, destination_path).await?;

        self.spawn_cache_layer(layer).await;

        Ok(())
    }

    pub async fn move_uploaded_outside_layer(
        self: Arc<Self>,
        layer: [u32; 5],
        file_name: &str,
    ) -> io::Result<()> {
        // nginx will pass in a full path to some file.  Since we want
        // to be at least somewhat security aware, we don't want this
        // to just accept any arbitrary path. The path needs to
        // actually live in what we know to be the upload path.
        let path: PathBuf = file_name.into();
        if path.parent().is_none() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "given file has no parent folder",
            ));
        }
        let parent = tokio::fs::canonicalize(path.parent().unwrap()).await?;
        let upload_path = tokio::fs::canonicalize(&self.upload_path).await?;
        if parent != upload_path {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "given file is not in upload folder",
            ));
        }
        self.move_uploaded_layer(layer, path).await
    }

    pub async fn spawn_cache_layer(self: Arc<Self>, layer: [u32; 5]) {
        tokio::spawn(try_copy_layer(self, layer));
    }

    async fn get_layer_header(
        self: Arc<Self>,
        layer: [u32; 5],
    ) -> std::io::Result<Option<(ArchiveHeader, File)>> {
        if let Some((_size, mut reader)) = self.get_layer_reader(layer).await? {
            Ok(Some((
                ArchiveHeader::parse_from_reader(&mut reader).await?,
                reader,
            )))
        } else {
            Ok(None)
        }
    }

    pub async fn get_layer_file_range(
        self: Arc<Self>,
        layer: [u32; 5],
        file: LayerFileEnum,
    ) -> std::io::Result<Option<Range<usize>>> {
        if let Some((header, mut reader)) = self.get_layer_header(layer).await? {
            let offset = reader.stream_position().await? as usize;
            Ok(header.range_for(file).map(|r| Range {
                start: r.start + offset,
                end: r.end + offset,
            }))
        } else {
            Ok(None)
        }
    }

    pub async fn get_layer_file(
        self: Arc<Self>,
        layer: [u32; 5],
        file: LayerFileEnum,
    ) -> std::io::Result<Option<(usize, impl Stream<Item = io::Result<Bytes>> + Send)>> {
        if let Some((header, mut reader)) = self.get_layer_header(layer).await? {
            if let Some(range) = header.range_for(file) {
                reader.seek(SeekFrom::Current(range.start as i64)).await?;
                let size = range.end - range.start;
                return Ok(Some((size, ReaderStream::new(reader.take(size as u64)))));
            }
        }

        Ok(None)
    }
}

async fn try_copy_layer(manager: Arc<LayerManager>, layer: [u32; 5]) {
    // critical region - check that we're not already copying this layer
    {
        let mut work_set = manager.work_set.lock().await;
        if work_set.contains(&layer) {
            return;
        }

        // final check to make sure that the file to be cached really doesn't exist
        // If the existence check fails, we'll just take that as a sign that we cannot cache.
        if manager.local_layer_file_exists(layer).await.unwrap_or(true) {
            return;
        }

        work_set.insert(layer);
    }

    let from = manager.primary_layer_file_path(layer);
    let to = manager.scratch_layer_file_path(layer);
    let mut result = tokio::fs::copy(from, &to).await.map(|_| ());
    if result.is_ok() {
        // we managed to copy the file over to the scratch dir.
        // It is now time to move it to the destination.
        //
        // The theory of this two stage strategy is that a move is
        // atomic, while a copy is not.  If we were to copy directly
        // to the destination, then a subsequent request could
        // accidentally return a partial file.
        //
        // This obviously only works if the scratch and the local are
        // on the same mount.
        //
        // this copy may actually fail, but we don't care.
        let dest = manager.local_layer_file_path(layer);
        if let Some(parent) = dest.parent() {
            result = tokio::fs::create_dir_all(parent).await;
        }
        if result.is_ok() {
            result = tokio::fs::rename(&to, dest).await;
        }
    }
    // remove from work set again
    let mut work_set = manager.work_set.lock().await;
    work_set.remove(&layer);

    if let Err(e) = result {
        eprintln!("Error: {e:?}");
    }
}
