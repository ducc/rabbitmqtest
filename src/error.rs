#[derive(Debug)]
pub enum Error {
    IoError(std::io::Error),
    Utf8Error(std::str::Utf8Error),
    PrometheusError(Box<prometheus::Error>),
    WarpError(warp::Error),
    LapinError(lapin::Error),
    JoinError(tokio::task::JoinError),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Error {
        Error::IoError(e)
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(e: std::str::Utf8Error) -> Error {
        Error::Utf8Error(e)
    }
}

impl From<prometheus::Error> for Error {
    fn from(e: prometheus::Error) -> Error {
        Error::PrometheusError(Box::new(e))
    }
}

impl From<warp::Error> for Error {
    fn from(e: warp::Error) -> Error {
        Error::WarpError(e)
    }
}

impl From<lapin::Error> for Error {
    fn from(e: lapin::Error) -> Error {
        Error::LapinError(e)
    }
}

impl From<tokio::task::JoinError> for Error {
    fn from(e: tokio::task::JoinError) -> Error {
        Error::JoinError(e)
    }
}

