//! Layer tars unpacked into overlay directories, by `hf-image-apply`: containerd's applier, in its own
//! process. The Rust untar here is for benchmarks only.

use std::collections::HashSet;
use std::io::{self, Read};
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, bail};
use sha2::{Digest, Sha256};

/// A reader that hashes what goes through it.
pub struct Hashing<R> {
    inner: R,
    hasher: Sha256,
}

impl<R: Read> Hashing<R> {
    pub fn new(inner: R) -> Self {
        Self { inner, hasher: Sha256::new() }
    }

    /// Reads to the end, then returns the `sha256:` digest of everything read.
    pub fn finish(mut self) -> io::Result<String> {
        io::copy(&mut self, &mut io::sink())?;
        Ok(format!("sha256:{}", hex::encode(self.hasher.finalize())))
    }
}

impl<R: Read> Read for Hashing<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.hasher.update(&buf[..n]);
        Ok(n)
    }
}

/// A layer unpacked into a directory.
#[derive(Debug)]
pub struct Unpacked {
    /// The tar's digest.
    pub diff_id: String,
    /// Directories created for entries under them, which the tar does not list.
    pub implicit: Vec<PathBuf>,
}

/// Unpacks the tar `reader` into `dir` with `applier` (`hf-image-apply`: containerd's applier),
/// which reads it on stdin.
pub fn apply(reader: impl Read, dir: &Path, applier: &Path) -> anyhow::Result<Unpacked> {
    use std::io::Write;
    use std::process::{Command, Stdio};
    let mut child = Command::new(applier)
        .arg(dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("failed to run {}", applier.display()))?;
    let mut stdin = child.stdin.take().context("no stdin")?;
    let mut stderr = child.stderr.take().context("no stderr")?;
    let errors = std::thread::spawn(move || {
        let mut s = String::new();
        let _ = stderr.read_to_string(&mut s);
        s
    });
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        unsafe { libc::fcntl(stdin.as_raw_fd(), libc::F_SETPIPE_SZ, 1 << 20) };
    }
    let mut hashing = Hashing::new(reader);
    let mut buf = vec![0; 1 << 20];
    let copied = (|| -> io::Result<()> {
        loop {
            let n = hashing.read(&mut buf)?;
            if n == 0 {
                return Ok(());
            }
            stdin.write_all(&buf[..n])?;
        }
    })();
    drop(stdin);
    let status = child.wait()?;
    let errors = errors.join().unwrap_or_default();
    if !status.success() {
        bail!("{} {status}: {}", applier.display(), errors.trim());
    }
    if !errors.trim().is_empty() {
        tracing::warn!(dir = %dir.display(), "{}", errors.trim());
    }
    copied.with_context(|| format!("failed to send the layer to {}", applier.display()))?;
    Ok(Unpacked { diff_id: hashing.finish()?, implicit: Vec::new() })
}

/// Unpacks the tar `reader` into `dir` in this process (`HF_IMAGE_RUST_UNTAR`, benchmarks only).
pub fn unpack(reader: impl Read, dir: &Path) -> anyhow::Result<Unpacked> {
    let mut hashing = Hashing::new(reader);
    let implicit = extract(&mut tar::Archive::new(&mut hashing), dir)?;
    Ok(Unpacked { diff_id: hashing.finish()?, implicit })
}

fn extract<R: Read>(archive: &mut tar::Archive<R>, dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
    archive.set_preserve_permissions(true);
    archive.set_preserve_ownerships(true);
    archive.set_preserve_mtime(true);
    archive.set_unpack_xattrs(true);
    archive.set_overwrite(true);
    let (mut known, mut implicit) = (HashSet::new(), HashSet::new());
    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = relative(&entry.path()?)?;
        for a in path.ancestors().skip(1).filter(|a| !a.as_os_str().is_empty()) {
            if !known.insert(a.to_path_buf()) {
                break;
            }
            implicit.insert(a.to_path_buf());
        }
        if entry.header().entry_type().is_dir() {
            known.insert(path.clone());
            implicit.remove(&path);
        }
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or_default();
        if let Some(hidden) = name.strip_prefix(".wh.") {
            let parent = dir.join(path.parent().unwrap_or(Path::new("")));
            std::fs::create_dir_all(&parent)?;
            if hidden == ".wh..opq" {
                opaque(&parent)?;
            } else {
                whiteout(&parent.join(hidden))?;
            }
            continue;
        }
        entry.unpack_in(dir).with_context(|| format!("failed to unpack {}", path.display()))?;
    }
    Ok(implicit.into_iter().collect())
}

/// Gives each `implicit` directory of `dir` the mode and owner of the nearest `lowers` copy, as
/// overlayfs' copy-up does; a whiteout in a lower ends the search.
pub fn inherit(dir: &Path, lowers: &[PathBuf], implicit: &[PathBuf]) -> anyhow::Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    for d in implicit {
        for lower in lowers {
            let Ok(m) = std::fs::symlink_metadata(lower.join(d)) else { continue };
            if m.is_dir() {
                let target = dir.join(d);
                std::os::unix::fs::lchown(&target, Some(m.uid()), Some(m.gid()))?;
                std::fs::set_permissions(&target, std::fs::Permissions::from_mode(m.mode() & 0o7777))?;
            }
            break;
        }
    }
    Ok(())
}

/// `path` without its root, refusing `..`.
fn relative(path: &Path) -> anyhow::Result<PathBuf> {
    let mut out = PathBuf::new();
    for c in path.components() {
        match c {
            Component::Normal(p) => out.push(p),
            Component::RootDir | Component::CurDir => {}
            _ => bail!("invalid path {}", path.display()),
        }
    }
    Ok(out)
}

#[cfg(target_os = "linux")]
fn cstr(path: &Path) -> anyhow::Result<std::ffi::CString> {
    use std::os::unix::ffi::OsStrExt;
    Ok(std::ffi::CString::new(path.as_os_str().as_bytes())?)
}

/// overlayfs' whiteout: a 0/0 character device.
#[cfg(target_os = "linux")]
fn whiteout(path: &Path) -> anyhow::Result<()> {
    let _ = std::fs::remove_file(path);
    let p = cstr(path)?;
    if unsafe { libc::mknod(p.as_ptr(), libc::S_IFCHR, libc::makedev(0, 0)) } != 0 {
        return Err(io::Error::last_os_error()).with_context(|| format!("whiteout {}", path.display()));
    }
    Ok(())
}

/// overlayfs' opaque directory: lower layers' entries are hidden.
#[cfg(target_os = "linux")]
fn opaque(dir: &Path) -> anyhow::Result<()> {
    let p = cstr(dir)?;
    let (name, value) = (c"trusted.overlay.opaque", b"y");
    if unsafe { libc::setxattr(p.as_ptr(), name.as_ptr(), value.as_ptr().cast(), 1, 0) } != 0 {
        return Err(io::Error::last_os_error()).with_context(|| format!("opaque {}", dir.display()));
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn whiteout(path: &Path) -> anyhow::Result<()> {
    bail!("whiteouts need Linux: {}", path.display())
}

#[cfg(not(target_os = "linux"))]
fn opaque(dir: &Path) -> anyhow::Result<()> {
    bail!("opaque directories need Linux: {}", dir.display())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_stay_inside() {
        assert_eq!(relative(Path::new("/a/b")).unwrap(), PathBuf::from("a/b"));
        assert_eq!(relative(Path::new("./a")).unwrap(), PathBuf::from("a"));
        assert!(relative(Path::new("a/../../b")).is_err());
    }

    fn layer() -> Vec<u8> {
        let mut b = tar::Builder::new(Vec::new());
        let mut h = tar::Header::new_gnu();
        h.set_size(5);
        h.set_mode(0o644);
        h.set_uid(unsafe { libc::getuid() }.into());
        h.set_gid(unsafe { libc::getgid() }.into());
        h.set_cksum();
        b.append_data(&mut h, "etc/hello", &b"hello"[..]).unwrap();
        b.into_inner().unwrap()
    }

    fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("untar-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// An applier that runs `script` with the target directory as `$1`.
    fn applier(script: &str) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let path = temp_dir().join("apply");
        std::fs::write(&path, format!("#!/bin/sh\n{script}\n")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    #[test]
    fn apply_sends_the_whole_tar_and_hashes_it() {
        let (bytes, dir) = (layer(), temp_dir());
        let unpacked = apply(&bytes[..], &dir, &applier(r#"cat > "$1/layer.tar""#)).unwrap();
        assert_eq!(unpacked.diff_id, crate::oci::sha256(&bytes));
        assert_eq!(std::fs::read(dir.join("layer.tar")).unwrap(), bytes);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn apply_fails_with_the_applier() {
        let dir = temp_dir();
        let err = apply(&layer()[..], &dir, &applier("echo refused >&2; exit 3")).unwrap_err();
        assert!(format!("{err:#}").contains("refused"), "{err:#}");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn unpacks_and_hashes() {
        let (bytes, dir) = (layer(), temp_dir());
        let unpacked = unpack(&bytes[..], &dir).unwrap();
        assert_eq!(unpacked.diff_id, crate::oci::sha256(&bytes));
        assert_eq!(unpacked.implicit, [PathBuf::from("etc")]);
        assert_eq!(std::fs::read(dir.join("etc/hello")).unwrap(), b"hello");
        std::fs::remove_dir_all(dir).unwrap();
    }
}
