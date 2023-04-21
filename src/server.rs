use hyper::{
    service::{make_service_fn, service_fn},
    Body, Method, Request, Response, Server, Uri,
};
use lazy_static::lazy_static;
use regex::Regex;
use std::{
    convert::Infallible,
    net::{IpAddr, Ipv6Addr, SocketAddr},
    path::PathBuf,
    sync::Arc,
};
use terminus_store::storage::{consts::LayerFileEnum, string_to_name};

use super::manager::LayerManager;

enum InvalidReason {
    BadMethod,
}

impl InvalidReason {
    fn status(&self) -> u16 {
        match self {
            Self::BadMethod => 400,
        }
    }
    fn msg(&self) -> Body {
        match self {
            Self::BadMethod => "invalid method".into(),
        }
    }
}

enum ResourceSpec {
    Cache([u32; 5]),
    Layer([u32; 5]),
    LayerFile([u32; 5], LayerFileEnum),
    LayerFileRange([u32; 5], LayerFileEnum),
}

#[derive(Debug)]
enum SpecParseError {
    UnknownPath,
    BadLayerName,
    UnknownLayerFile,
}

fn uri_to_spec(uri: &Uri) -> Result<ResourceSpec, SpecParseError> {
    lazy_static! {
        static ref RE_CACHE: Regex = Regex::new(r"^/cache/([0-9a-f]{40})$").unwrap();
        static ref RE_LAYER: Regex = Regex::new(r"^/layer/([0-9a-f]{40})$").unwrap();
        static ref RE_FILE: Regex = Regex::new(r"^/file/([0-9a-f]{40})/(\w+)$").unwrap();
        static ref RE_FILE_RANGE: Regex = Regex::new(r"^/range/([0-9a-f]{40})/(\w+)$").unwrap();
    }
    let path = uri.path();

    if let Some(captures) = RE_CACHE.captures(path) {
        let name = captures.get(1).unwrap();
        Ok(ResourceSpec::Cache(
            string_to_name(name.as_str()).map_err(|_e| SpecParseError::BadLayerName)?,
        ))
    } else if let Some(captures) = RE_LAYER.captures(path) {
        let name = captures.get(1).unwrap();
        Ok(ResourceSpec::Layer(
            string_to_name(name.as_str()).map_err(|_e| SpecParseError::BadLayerName)?,
        ))
    } else if let Some(captures) = RE_FILE.captures(path) {
        let layer_name = captures.get(1).unwrap();
        let file_name = captures.get(2).unwrap();
        let layer_name =
            string_to_name(layer_name.as_str()).map_err(|_e| SpecParseError::BadLayerName)?;
        if let Some(file) = file_name_to_enum(file_name.as_str()) {
            Ok(ResourceSpec::LayerFile(layer_name, file))
        } else {
            Err(SpecParseError::UnknownLayerFile)
        }
    } else if let Some(captures) = RE_FILE_RANGE.captures(path) {
        let layer_name = captures.get(1).unwrap();
        let file_name = captures.get(2).unwrap();
        let layer_name =
            string_to_name(layer_name.as_str()).map_err(|_e| SpecParseError::BadLayerName)?;
        if let Some(file) = file_name_to_enum(file_name.as_str()) {
            Ok(ResourceSpec::LayerFileRange(layer_name, file))
        } else {
            Err(SpecParseError::UnknownLayerFile)
        }
    } else {
        eprintln!("{uri:?}");
        Err(SpecParseError::UnknownPath)
    }
}

struct Service {
    manager: Arc<LayerManager>,
}

impl Service {
    fn new<P1: Into<PathBuf>, P2: Into<PathBuf>, P3: Into<PathBuf>, P4: Into<PathBuf>>(
        primary_path: P1,
        local_path: P2,
        upload_path: P3,
        scratch_path: P4,
    ) -> Self {
        Service {
            manager: Arc::new(LayerManager::new(
                primary_path,
                local_path,
                upload_path,
                scratch_path,
            )),
        }
    }
    async fn serve(&self, req: Request<Body>) -> Result<Response<Body>, Infallible> {
        match req.method() {
            &Method::GET => self.get(req).await,
            &Method::POST => self.post(req).await,
            _ => self.invalid(req, InvalidReason::BadMethod).await,
        }
    }

    async fn get(&self, req: Request<Body>) -> Result<Response<Body>, Infallible> {
        let spec = uri_to_spec(req.uri());
        match spec {
            Ok(ResourceSpec::Layer(layer)) => match self.manager.clone().get_layer(layer).await {
                Ok(Some((size, stream))) => Ok(Response::builder()
                    .header("Content-Length", size)
                    .body(Body::wrap_stream(stream))
                    .unwrap()),
                Ok(None) => Ok(Response::builder()
                    .status(404)
                    .body("Layer not found".into())
                    .unwrap()),
                Err(e) => Ok(Response::builder()
                    .status(500)
                    .body(format!("Error: {e}").into())
                    .unwrap()),
            },
            Ok(ResourceSpec::LayerFile(layer, file)) => {
                match self.manager.clone().get_layer_file(layer, file).await {
                    Ok(Some((size, stream))) => Ok(Response::builder()
                        .header("Content-Length", size)
                        .body(Body::wrap_stream(stream))
                        .unwrap()),
                    Ok(None) => Ok(Response::builder().status(404).body(Body::empty()).unwrap()),
                    Err(e) => Ok(Response::builder()
                        .status(500)
                        .body(format!("Error: {e}").into())
                        .unwrap()),
                }
            }
            Ok(ResourceSpec::LayerFileRange(layer, file)) => {
                match self.manager.clone().get_layer_file_range(layer, file).await {
                    Ok(Some(range)) => Ok(Response::builder()
                        .body(format!("{}-{}", range.start, range.end - 1).into())
                        .unwrap()),
                    Ok(None) => Ok(Response::builder().status(404).body(Body::empty()).unwrap()),
                    Err(e) => Ok(Response::builder()
                        .status(500)
                        .body(format!("Error: {e}").into())
                        .unwrap()),
                }
            }
            Ok(_) => Ok(Response::builder()
                .status(500)
                .body("Unimplemented".into())
                .unwrap()),
            Err(e) => Ok(Response::builder()
                .status(500)
                .body(format!("Error: {e:?}").into())
                .unwrap()),
        }
    }
    async fn post(&self, mut req: Request<Body>) -> Result<Response<Body>, Infallible> {
        let spec = uri_to_spec(req.uri());
        match spec {
            Ok(ResourceSpec::Cache(layer)) => {
                self.manager.clone().spawn_cache_layer(layer).await;
                Ok(Response::builder().status(204).body(Body::empty()).unwrap())
            }
            Ok(ResourceSpec::Layer(layer)) => {
                match self
                    .manager
                    .clone()
                    .upload_layer(layer, req.body_mut())
                    .await
                {
                    Ok(()) => Ok(Response::builder().status(204).body(Body::empty()).unwrap()),
                    Err(e) => Ok(Response::builder()
                        .status(500)
                        .body(format!("Error: {e:?}").into())
                        .unwrap()),
                }
            }
            Ok(_) => Ok(Response::builder()
                .status(500)
                .body("Unimplemented".into())
                .unwrap()),
            Err(e) => Ok(Response::builder()
                .status(500)
                .body(format!("Error: {e:?}").into())
                .unwrap()),
        }
    }

    async fn invalid(
        &self,
        _req: Request<Body>,
        reason: InvalidReason,
    ) -> Result<Response<Body>, Infallible> {
        Ok(Response::builder()
            .status(reason.status())
            .body(reason.msg())
            .unwrap())
    }
}

pub fn file_name_to_enum(name: &str) -> Option<LayerFileEnum> {
    let result = match name {
        "node_dictionary_blocks" => LayerFileEnum::NodeDictionaryBlocks,
        "node_dictionary_offsets" => LayerFileEnum::NodeDictionaryOffsets,
        "predicate_dictionary_blocks" => LayerFileEnum::PredicateDictionaryBlocks,
        "predicate_dictionary_offsets" => LayerFileEnum::PredicateDictionaryOffsets,
        "value_dictionary_types_present" => LayerFileEnum::ValueDictionaryTypesPresent,
        "value_dictionary_type_offsets" => LayerFileEnum::ValueDictionaryTypeOffsets,
        "value_dictionary_blocks" => LayerFileEnum::ValueDictionaryBlocks,
        "value_dictionary_offsets" => LayerFileEnum::ValueDictionaryOffsets,

        "node_value_id_map_bits" => LayerFileEnum::NodeValueIdMapBits,
        "node_value_id_map_bit_index_blocks" => LayerFileEnum::NodeValueIdMapBitIndexBlocks,
        "node_value_id_map_bit_index_sblocks" => LayerFileEnum::NodeValueIdMapBitIndexSBlocks,

        "predicate_id_map_bits" => LayerFileEnum::PredicateIdMapBits,
        "predicate_id_map_bit_index_blocks" => LayerFileEnum::PredicateIdMapBitIndexBlocks,
        "predicate_id_map_bit_index_sblocks" => LayerFileEnum::PredicateIdMapBitIndexSBlocks,

        "pos_subjects" => LayerFileEnum::PosSubjects,
        "pos_objects" => LayerFileEnum::PosObjects,
        "neg_subjects" => LayerFileEnum::NegSubjects,
        "neg_objects" => LayerFileEnum::NegObjects,

        "pos_sp_adjacency_list_nums" => LayerFileEnum::PosSPAdjacencyListNums,
        "pos_sp_adjacency_list_bits" => LayerFileEnum::PosSPAdjacencyListBits,
        "pos_sp_adjacency_list_bit_index_blocks" => LayerFileEnum::PosSPAdjacencyListBitIndexBlocks,
        "pos_sp_adjacency_list_bit_index_sblocks" => {
            LayerFileEnum::PosSPAdjacencyListBitIndexSBlocks
        }
        "pos_sp_o_adjacency_list_nums" => LayerFileEnum::PosSpOAdjacencyListNums,
        "pos_sp_o_adjacency_list_bits" => LayerFileEnum::PosSpOAdjacencyListBits,
        "pos_sp_o_adjacency_list_bit_index_blocks" => {
            LayerFileEnum::PosSpOAdjacencyListBitIndexBlocks
        }
        "pos_sp_o_adjacency_list_bit_index_sblocks" => {
            LayerFileEnum::PosSpOAdjacencyListBitIndexSBlocks
        }

        "pos_o_ps_adjacency_list_nums" => LayerFileEnum::PosOPsAdjacencyListNums,
        "pos_o_ps_adjacency_list_bits" => LayerFileEnum::PosOPsAdjacencyListBits,
        "pos_o_ps_adjacency_list_bit_index_blocks" => {
            LayerFileEnum::PosOPsAdjacencyListBitIndexBlocks
        }
        "pos_o_ps_adjacency_list_bit_index_sblocks" => {
            LayerFileEnum::PosOPsAdjacencyListBitIndexSBlocks
        }
        "pos_predicate_wavelet_tree_bits" => LayerFileEnum::PosPredicateWaveletTreeBits,
        "pos_predicate_wavelet_tree_bit_index_blocks" => {
            LayerFileEnum::PosPredicateWaveletTreeBitIndexBlocks
        }
        "pos_predicate_wavelet_tree_bit_index_sblocks" => {
            LayerFileEnum::PosPredicateWaveletTreeBitIndexSBlocks
        }

        "neg_sp_adjacency_list_nums" => LayerFileEnum::NegSPAdjacencyListNums,
        "neg_sp_adjacency_list_bits" => LayerFileEnum::NegSPAdjacencyListBits,
        "neg_sp_adjacency_list_bit_index_blocks" => LayerFileEnum::NegSPAdjacencyListBitIndexBlocks,
        "neg_sp_adjacency_list_bit_index_sblocks" => {
            LayerFileEnum::NegSPAdjacencyListBitIndexSBlocks
        }
        "neg_sp_o_adjacency_list_nums" => LayerFileEnum::NegSpOAdjacencyListNums,
        "neg_sp_o_adjacency_list_bits" => LayerFileEnum::NegSpOAdjacencyListBits,
        "neg_sp_o_adjacency_list_bit_index_blocks" => {
            LayerFileEnum::NegSpOAdjacencyListBitIndexBlocks
        }
        "neg_sp_o_adjacency_list_bit_index_sblocks" => {
            LayerFileEnum::NegSpOAdjacencyListBitIndexSBlocks
        }

        "neg_o_ps_adjacency_list_nums" => LayerFileEnum::NegOPsAdjacencyListNums,
        "neg_o_ps_adjacency_list_bits" => LayerFileEnum::NegOPsAdjacencyListBits,
        "neg_o_ps_adjacency_list_bit_index_blocks" => {
            LayerFileEnum::NegOPsAdjacencyListBitIndexBlocks
        }
        "neg_o_ps_adjacency_list_bit_index_sblocks" => {
            LayerFileEnum::NegOPsAdjacencyListBitIndexSBlocks
        }
        "neg_predicate_wavelet_tree_bits" => LayerFileEnum::NegPredicateWaveletTreeBits,
        "neg_predicate_wavelet_tree_bit_index_blocks" => {
            LayerFileEnum::NegPredicateWaveletTreeBitIndexBlocks
        }
        "neg_predicate_wavelet_tree_bit_index_sblocks" => {
            LayerFileEnum::NegPredicateWaveletTreeBitIndexSBlocks
        }

        "parent" => LayerFileEnum::Parent,
        _ => return None,
    };

    Some(result)
}

#[allow(unused)]
pub fn file_enum_to_string(file: LayerFileEnum) -> Option<&'static str> {
    let result = match file {
        LayerFileEnum::NodeDictionaryBlocks => "node_dictionary_blocks",
        LayerFileEnum::NodeDictionaryOffsets => "node_dictionary_offsets",
        LayerFileEnum::PredicateDictionaryBlocks => "predicate_dictionary_blocks",
        LayerFileEnum::PredicateDictionaryOffsets => "predicate_dictionary_offsets",
        LayerFileEnum::ValueDictionaryTypesPresent => "value_dictionary_types_present",
        LayerFileEnum::ValueDictionaryTypeOffsets => "value_dictionary_type_offsets",
        LayerFileEnum::ValueDictionaryBlocks => "value_dictionary_blocks",
        LayerFileEnum::ValueDictionaryOffsets => "value_dictionary_offsets",

        LayerFileEnum::NodeValueIdMapBits => "node_value_id_map_bits",
        LayerFileEnum::NodeValueIdMapBitIndexBlocks => "node_value_id_map_bit_index_blocks",
        LayerFileEnum::NodeValueIdMapBitIndexSBlocks => "node_value_id_map_bit_index_sblocks",

        LayerFileEnum::PredicateIdMapBits => "predicate_id_map_bits",
        LayerFileEnum::PredicateIdMapBitIndexBlocks => "predicate_id_map_bit_index_blocks",
        LayerFileEnum::PredicateIdMapBitIndexSBlocks => "predicate_id_map_bit_index_sblocks",

        LayerFileEnum::PosSubjects => "pos_subjects",
        LayerFileEnum::PosObjects => "pos_objects",
        LayerFileEnum::NegSubjects => "neg_subjects",
        LayerFileEnum::NegObjects => "neg_objects",

        LayerFileEnum::PosSPAdjacencyListNums => "pos_sp_adjacency_list_nums",
        LayerFileEnum::PosSPAdjacencyListBits => "pos_sp_adjacency_list_bits",
        LayerFileEnum::PosSPAdjacencyListBitIndexBlocks => "pos_sp_adjacency_list_bit_index_blocks",
        LayerFileEnum::PosSPAdjacencyListBitIndexSBlocks => {
            "pos_sp_adjacency_list_bit_index_sblocks"
        }
        LayerFileEnum::PosSpOAdjacencyListNums => "pos_sp_o_adjacency_list_nums",
        LayerFileEnum::PosSpOAdjacencyListBits => "pos_sp_o_adjacency_list_bits",
        LayerFileEnum::PosSpOAdjacencyListBitIndexBlocks => {
            "pos_sp_o_adjacency_list_bit_index_blocks"
        }
        LayerFileEnum::PosSpOAdjacencyListBitIndexSBlocks => {
            "pos_sp_o_adjacency_list_bit_index_sblocks"
        }

        LayerFileEnum::PosOPsAdjacencyListNums => "pos_o_ps_adjacency_list_nums",
        LayerFileEnum::PosOPsAdjacencyListBits => "pos_o_ps_adjacency_list_bits",
        LayerFileEnum::PosOPsAdjacencyListBitIndexBlocks => {
            "pos_o_ps_adjacency_list_bit_index_blocks"
        }
        LayerFileEnum::PosOPsAdjacencyListBitIndexSBlocks => {
            "pos_o_ps_adjacency_list_bit_index_sblocks"
        }
        LayerFileEnum::PosPredicateWaveletTreeBits => "pos_predicate_wavelet_tree_bits",
        LayerFileEnum::PosPredicateWaveletTreeBitIndexBlocks => {
            "pos_predicate_wavelet_tree_bit_index_blocks"
        }
        LayerFileEnum::PosPredicateWaveletTreeBitIndexSBlocks => {
            "pos_predicate_wavelet_tree_bit_index_sblocks"
        }

        LayerFileEnum::NegSPAdjacencyListNums => "neg_sp_adjacency_list_nums",
        LayerFileEnum::NegSPAdjacencyListBits => "neg_sp_adjacency_list_bits",
        LayerFileEnum::NegSPAdjacencyListBitIndexBlocks => "neg_sp_adjacency_list_bit_index_blocks",
        LayerFileEnum::NegSPAdjacencyListBitIndexSBlocks => {
            "neg_sp_adjacency_list_bit_index_sblocks"
        }
        LayerFileEnum::NegSpOAdjacencyListNums => "neg_sp_o_adjacency_list_nums",
        LayerFileEnum::NegSpOAdjacencyListBits => "neg_sp_o_adjacency_list_bits",
        LayerFileEnum::NegSpOAdjacencyListBitIndexBlocks => {
            "neg_sp_o_adjacency_list_bit_index_blocks"
        }
        LayerFileEnum::NegSpOAdjacencyListBitIndexSBlocks => {
            "neg_sp_o_adjacency_list_bit_index_sblocks"
        }

        LayerFileEnum::NegOPsAdjacencyListNums => "neg_o_ps_adjacency_list_nums",
        LayerFileEnum::NegOPsAdjacencyListBits => "neg_o_ps_adjacency_list_bits",
        LayerFileEnum::NegOPsAdjacencyListBitIndexBlocks => {
            "neg_o_ps_adjacency_list_bit_index_blocks"
        }
        LayerFileEnum::NegOPsAdjacencyListBitIndexSBlocks => {
            "neg_o_ps_adjacency_list_bit_index_sblocks"
        }
        LayerFileEnum::NegPredicateWaveletTreeBits => "neg_predicate_wavelet_tree_bits",
        LayerFileEnum::NegPredicateWaveletTreeBitIndexBlocks => {
            "neg_predicate_wavelet_tree_bit_index_blocks"
        }
        LayerFileEnum::NegPredicateWaveletTreeBitIndexSBlocks => {
            "neg_predicate_wavelet_tree_bit_index_sblocks"
        }

        LayerFileEnum::Parent => "parent",
        _ => return None,
    };

    Some(result)
}

pub async fn serve<P1: Into<PathBuf>, P2: Into<PathBuf>, P3: Into<PathBuf>, P4: Into<PathBuf>>(
    primary_path: P1,
    local_path: P2,
    upload_path: P3,
    scratch_path: P4,
    port: u16,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), port);
    let service = Arc::new(Service::new(
        primary_path,
        local_path,
        upload_path,
        scratch_path,
    ));

    let make_svc = make_service_fn(move |_conn| {
        let s = service.clone();
        async {
            Ok::<_, Infallible>(service_fn(move |req| {
                let s = s.clone();
                async move { s.serve(req).await }
            }))
        }
    });

    let server = Server::bind(&addr).serve(make_svc);
    server.await?;

    Ok(())
}
