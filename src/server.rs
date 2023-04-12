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
use terminus_store::storage::{consts::LayerFileEnum, name_to_string, string_to_name};

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
    Layer([u32; 5]),
    LayerFile([u32; 5], LayerFileEnum),
}

#[derive(Debug)]
enum SpecParseError {
    UnknownPath,
    BadLayerName,
    UnknownLayerFile,
}

fn uri_to_spec(uri: &Uri) -> Result<ResourceSpec, SpecParseError> {
    lazy_static! {
        static ref RE_LAYER: Regex = Regex::new(r"^/([0-9a-f]{40})$").unwrap();
        static ref RE_FILE: Regex = Regex::new(r"^/([0-9a-f]{40})/(\w+)$").unwrap();
    }
    let path = uri.path();

    if let Some(captures) = RE_LAYER.captures(path) {
        let name = captures.get(1).unwrap();
        Ok(ResourceSpec::Layer(
            string_to_name(name.as_str()).map_err(|e| SpecParseError::BadLayerName)?,
        ))
    } else if let Some(captures) = RE_FILE.captures(path) {
        Err(SpecParseError::UnknownLayerFile)
    } else {
        eprintln!("{uri:?}");
        Err(SpecParseError::UnknownPath)
    }
}

struct Service {
    manager: Arc<LayerManager>,
}

impl Service {
    fn new<P1: Into<PathBuf>, P2: Into<PathBuf>, P3: Into<PathBuf>>(
        primary_path: P1,
        local_path: P2,
        scratch_path: P3,
    ) -> Self {
        Service {
            manager: Arc::new(LayerManager::new(primary_path, local_path, scratch_path)),
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
                Ok(Some(stream)) => Ok(Response::new(Body::wrap_stream(stream))),
                Ok(None) => Ok(Response::builder()
                    .status(404)
                    .body("Layer not found".into())
                    .unwrap()),
                Err(e) => Ok(Response::builder()
                    .status(500)
                    .body(format!("Error: {e}").into())
                    .unwrap()),
            },
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
    async fn post(&self, _req: Request<Body>) -> Result<Response<Body>, Infallible> {
        Ok(Response::new("it was a post".into()))
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

pub async fn serve<P1: Into<PathBuf>, P2: Into<PathBuf>, P3: Into<PathBuf>>(
    primary_path: P1,
    local_path: P2,
    scratch_path: P3,
    port: u16,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), port);
    let service = Arc::new(Service::new(primary_path, local_path, scratch_path));

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
