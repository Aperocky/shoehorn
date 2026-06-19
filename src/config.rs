use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub struct Config {
    pub log_path: Option<PathBuf>,
    pub listen_addr: String,
    pub socks_addr: String,
}

impl Config {
    pub fn load() -> io::Result<Self> {
        let path = config_path()?;
        let contents = fs::read_to_string(&path)?;
        parse(&contents).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to parse {}: {error}", path.display()),
            )
        })
    }
}

#[derive(Clone)]
pub struct Logger {
    file: Option<Arc<Mutex<File>>>,
}

impl Logger {
    pub fn new(path: Option<&Path>) -> io::Result<Self> {
        let file = match path {
            Some(path) => {
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)?;
                }

                Some(Arc::new(Mutex::new(
                    OpenOptions::new().create(true).append(true).open(path)?,
                )))
            }
            None => None,
        };

        Ok(Self { file })
    }

    pub fn info(&self, message: impl AsRef<str>) {
        self.log("INFO", message.as_ref());
    }

    pub fn error(&self, message: impl AsRef<str>) {
        self.log("ERROR", message.as_ref());
    }

    fn log(&self, level: &str, message: &str) {
        let line = format!("{} {level} {message}\n", timestamp_millis());
        eprint!("{line}");

        if let Some(file) = &self.file {
            match file.lock() {
                Ok(mut file) => {
                    if let Err(error) = file.write_all(line.as_bytes()) {
                        eprintln!("failed to write log file: {error}");
                    }
                }
                Err(error) => eprintln!("failed to lock log file: {error}"),
            }
        }
    }
}

fn parse(contents: &str) -> Result<Config, String> {
    let mut log_path = None;
    let mut listen_addr = None;
    let mut socks_addr = None;

    for (index, raw_line) in contents.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let Some((key, value)) = line.split_once('=') else {
            return Err(format!("line {} is missing '='", index + 1));
        };
        let key = key.trim();
        let value = value.trim();

        if value.is_empty() {
            return Err(format!("line {} has an empty value for {key}", index + 1));
        }

        match key {
            "log_path" => log_path = Some(PathBuf::from(value)),
            "listen_addr" => listen_addr = Some(value.to_owned()),
            "socks_addr" => socks_addr = Some(value.to_owned()),
            _ => return Err(format!("line {} has unknown key {key}", index + 1)),
        }
    }

    Ok(Config {
        log_path,
        listen_addr: listen_addr.ok_or_else(|| "listen_addr is required".to_owned())?,
        socks_addr: socks_addr.ok_or_else(|| "socks_addr is required".to_owned())?,
    })
}

fn config_path() -> io::Result<PathBuf> {
    let home = env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "HOME is not set"))?;

    Ok(home.join(".config").join("shoehorn").join("shoehorn.conf"))
}

fn timestamp_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}
