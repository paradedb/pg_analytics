use std::{
    ffi::OsStr,
    fs,
    path::{Path, PathBuf},
};

use anyhow::Result;
use async_std::task::{self, block_on};
use datafusion::common::runtime::SpawnedTask;
use futures::StreamExt;
use log::info;

use paradedb_sqllogictest::engine::ParadeDB;
use sqllogictest::strict_column_validator;

const TEST_DIRECTORY: &str = "test_files/";

pub fn main() -> Result<()> {
    block_on(run_tests())
}

async fn run_tests() -> Result<()> {
    // Enable logging (e.g. set RUST_LOG=debug to see debug logs)
    // env_logger::init();

    // let options: Options = clap::Parser::parse();
    // options.warn_on_ignored();

    // Run all tests in parallel, reporting failures at the end
    //
    // Doing so is safe because each slt file runs with its own
    // `SessionContext` and should not have side effects (like
    // modifying shared state like `/tmp/`)
    let errors: Vec<_> = futures::stream::iter(read_test_files()?)
        .map(|test_file| {
            SpawnedTask::spawn(async move {
                println!("Running {:?}", test_file.relative_path);
                run_test_file(test_file).await?;
                Ok(()) as Result<()>
            })
            .join()
        })
        // run up to num_cpus streams in parallel
        .buffer_unordered(num_cpus::get())
        .flat_map(|result| {
            // Filter out any Ok() leaving only the DataFusionErrors
            futures::stream::iter(match result {
                // Tokio panic error
                Err(e) => Some(anyhow::anyhow!("{e}")),
                Ok(thread_result) => match thread_result {
                    // Test run error
                    Err(e) => Some(e),
                    // success
                    Ok(_) => None,
                },
            })
        })
        .collect()
        .await;

    // report on any errors
    if !errors.is_empty() {
        for e in &errors {
            println!("{e}");
        }
        anyhow::bail!("{} errors occurred", errors.len());
    } else {
        Ok(())
    }
}

fn read_test_files<'a>() -> Result<Box<dyn Iterator<Item = TestFile> + 'a>> {
    Ok(Box::new(
        read_dir_recursive(TEST_DIRECTORY)?
            .into_iter()
            .map(TestFile::new)
            .filter(|f| f.is_slt_file()),
    ))
}

fn read_dir_recursive<P: AsRef<Path>>(path: P) -> Result<Vec<PathBuf>> {
    let mut dst = vec![];
    read_dir_recursive_impl(&mut dst, path.as_ref())?;
    Ok(dst)
}

/// Append all paths recursively to dst
fn read_dir_recursive_impl(dst: &mut Vec<PathBuf>, path: &Path) -> Result<()> {
    let entries = std::fs::read_dir(path)
        .map_err(|e| anyhow::anyhow!("Error reading directory {path:?}: {e}"))?;
    for entry in entries {
        let path = entry
            .map_err(|e| anyhow::anyhow!("Error reading entry in directory {path:?}: {e}"))?
            .path();

        if path.is_dir() {
            read_dir_recursive_impl(dst, &path)?;
        } else {
            dst.push(path);
        }
    }

    Ok(())
}

async fn run_test_file(test_file: TestFile) -> Result<()> {
    let TestFile {
        path,
        relative_path,
    } = test_file;
    info!("Running with ParadeDB runner: {}", path.display());

    // let Some(test_ctx) = TestContext::try_new_for_test_file(&relative_path).await else {
    //     info!("Skipping: {}", path.display());
    //     return Ok(());
    // };

    setup_scratch_dir(&relative_path)?;
    let mut runner = sqllogictest::Runner::new(|| async { Ok(ParadeDB::new().await) });

    runner.with_column_validator(strict_column_validator);

    runner
        .run_file_async(path)
        .await
        .map_err(|e| anyhow::anyhow!("Error running test file: {}", e))
}

/// Represents a parsed test file
#[derive(Debug)]
struct TestFile {
    /// The absolute path to the file
    pub path: PathBuf,
    /// The relative path of the file (used for display)
    pub relative_path: PathBuf,
}

impl TestFile {
    fn new(path: PathBuf) -> Self {
        let relative_path = PathBuf::from(
            path.to_string_lossy()
                .strip_prefix(TEST_DIRECTORY)
                .unwrap_or(""),
        );

        Self {
            path,
            relative_path,
        }
    }

    fn is_slt_file(&self) -> bool {
        self.path.extension() == Some(OsStr::new("slt"))
    }
}

/// Sets up an empty directory at test_files/scratch/<name>
/// creating it if needed and clearing any file contents if it exists
/// This allows tests for inserting to external tables or copy to
/// to persist data to disk and have consistent state when running
/// a new test
fn setup_scratch_dir(name: &Path) -> Result<()> {
    // go from copy.slt --> copy
    let file_stem = name.file_stem().expect("File should have a stem");
    let path = PathBuf::from("test_files").join("scratch").join(file_stem);

    info!("Creating scratch dir in {path:?}");
    if path.exists() {
        fs::remove_dir_all(&path)?;
    }
    fs::create_dir_all(&path)?;
    Ok(())
}
