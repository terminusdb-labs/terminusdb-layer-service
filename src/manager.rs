use std::{
    collections::HashSet,
    io::{self, ErrorKind},
    os::unix::prelude::MetadataExt,
    path::PathBuf,
    sync::Arc,
};

use bytes::Bytes;
use futures::{future::Either, Stream};
use terminus_store::storage::name_to_string;
use tokio::sync::Mutex;
use tokio_util::io::ReaderStream;

pub struct LayerManager {
    primary_path: PathBuf,
    local_path: PathBuf,
    scratch_path: PathBuf,
    work_set: Mutex<HashSet<[u32; 5]>>,
}

impl LayerManager {
    pub fn new<P1: Into<PathBuf>, P2: Into<PathBuf>, P3: Into<PathBuf>>(
        primary_path: P1,
        local_path: P2,
        scratch_path: P3,
    ) -> Self {
        LayerManager {
            primary_path: primary_path.into(),
            local_path: local_path.into(),
            scratch_path: scratch_path.into(),
            work_set: Mutex::new(HashSet::new()),
        }
    }

    async fn file_stream(
        &self,
        path: &PathBuf,
    ) -> std::io::Result<Option<(usize, impl Stream<Item = io::Result<Bytes>> + Send)>> {
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
            Ok(r) => Ok(Some((size, ReaderStream::new(r)))),
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

    async fn primary_layer_file_stream(
        &self,
        layer: [u32; 5],
    ) -> std::io::Result<Option<(usize, impl Stream<Item = io::Result<Bytes>> + Send)>> {
        let path = self.primary_layer_file_path(layer);
        self.file_stream(&path).await
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

    async fn local_layer_file_stream(
        &self,
        layer: [u32; 5],
    ) -> std::io::Result<Option<(usize, impl Stream<Item = io::Result<Bytes>> + Send)>> {
        let path = self.local_layer_file_path(layer);

        self.file_stream(&path).await
    }

    fn scratch_layer_file_path(&self, layer: [u32; 5]) -> PathBuf {
        let mut path = self.scratch_path.clone();
        let name = name_to_string(layer);
        path.push(format!("{name}.larch"));

        path
    }

    pub async fn get_layer(
        self: Arc<Self>,
        layer: [u32; 5],
    ) -> std::io::Result<Option<(usize, impl Stream<Item = io::Result<Bytes>> + Send)>> {
        if let Some((size, stream)) = self.local_layer_file_stream(layer).await? {
            Ok(Some((size, Either::Left(stream))))
        } else if let Some((size, stream)) = self.primary_layer_file_stream(layer).await? {
            // attempt to cache this file
            self.clone().spawn_cache_layer(layer).await;
            tokio::spawn(try_copy_layer(self.clone(), layer));
            Ok(Some((size, Either::Right(stream))))
        } else {
            Ok(None)
        }
    }

    pub async fn spawn_cache_layer(self: Arc<Self>, layer: [u32; 5]) {
        tokio::spawn(try_copy_layer(self, layer));
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
