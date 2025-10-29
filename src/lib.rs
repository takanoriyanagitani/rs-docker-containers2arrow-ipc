use std::io;
use std::sync::Arc;

use io::BufWriter;
use io::Write;

use arrow::record_batch::RecordBatch;

use bollard::ClientVersion;
use bollard::Docker;
use bollard::models::ContainerSummary;
use bollard::query_parameters::ListContainersOptions;

pub async fn list_containers(
    d: &Docker,
    opts: Option<ListContainersOptions>,
) -> Result<Vec<ContainerSummary>, io::Error> {
    d.list_containers(opts).await.map_err(io::Error::other)
}

use arrow::array::ArrayRef;
use arrow::array::builder::{Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};

pub fn summary_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("image", DataType::Utf8, true),
        Field::new("image_id", DataType::Utf8, true),
        Field::new("command", DataType::Utf8, true),
        Field::new("state", DataType::Utf8, true),
        Field::new("status", DataType::Utf8, true),
        Field::new("created", DataType::Int64, true),
        Field::new("size_rw", DataType::Int64, true),
        Field::new("size_root_fs", DataType::Int64, true),
    ]))
}

pub fn containers2batch(
    containers: Vec<ContainerSummary>,
    schema: Arc<Schema>,
) -> Result<RecordBatch, io::Error> {
    let mut id_builder = StringBuilder::new();
    let mut image_builder = StringBuilder::new();
    let mut image_id_builder = StringBuilder::new();
    let mut command_builder = StringBuilder::new();
    let mut state_builder = StringBuilder::new();
    let mut status_builder = StringBuilder::new();
    let mut created_builder = Int64Builder::new();
    let mut size_rw_builder = Int64Builder::new();
    let mut size_root_fs_builder = Int64Builder::new();

    for container in containers {
        id_builder.append_option(container.id);
        image_builder.append_option(container.image);
        image_id_builder.append_option(container.image_id);
        command_builder.append_option(container.command);
        state_builder.append_option(container.state);
        status_builder.append_option(container.status);
        created_builder.append_option(container.created);
        size_rw_builder.append_option(container.size_rw);
        size_root_fs_builder.append_option(container.size_root_fs);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(id_builder.finish()),
        Arc::new(image_builder.finish()),
        Arc::new(image_id_builder.finish()),
        Arc::new(command_builder.finish()),
        Arc::new(state_builder.finish()),
        Arc::new(status_builder.finish()),
        Arc::new(created_builder.finish()),
        Arc::new(size_rw_builder.finish()),
        Arc::new(size_root_fs_builder.finish()),
    ];

    RecordBatch::try_new(schema, columns).map_err(io::Error::other)
}

pub struct IpcStreamWriter<W>(pub arrow::ipc::writer::StreamWriter<BufWriter<W>>)
where
    W: Write;

impl<W> IpcStreamWriter<W>
where
    W: Write,
{
    pub fn finish(&mut self) -> Result<(), io::Error> {
        self.0.finish().map_err(io::Error::other)
    }

    pub fn flush(&mut self) -> Result<(), io::Error> {
        self.0.flush().map_err(io::Error::other)
    }

    pub fn write_batch(&mut self, b: &RecordBatch) -> Result<(), io::Error> {
        self.0.write(b).map_err(io::Error::other)
    }
}

pub fn batch2writer<W>(b: &RecordBatch, mut wtr: W, sch: &Schema) -> Result<(), io::Error>
where
    W: Write,
{
    let swtr = arrow::ipc::writer::StreamWriter::try_new_buffered(&mut wtr, sch)
        .map_err(io::Error::other)?;
    let mut iw = IpcStreamWriter(swtr);
    iw.write_batch(b)?;
    iw.flush()?;
    iw.finish()?;

    drop(iw);

    wtr.flush()
}

pub fn containers2writer<W>(
    containers: Vec<ContainerSummary>,
    mut wtr: W,
    sch: Arc<Schema>,
) -> Result<(), io::Error>
where
    W: Write,
{
    let batch = containers2batch(containers, sch.clone())?;
    batch2writer(&batch, &mut wtr, &sch)
}

pub async fn list_containers_and_write<W>(
    d: &Docker,
    mut wtr: W,
    opts: Option<ListContainersOptions>,
) -> Result<(), io::Error>
where
    W: Write,
{
    let containers = list_containers(d, opts).await?;
    let schema = summary_schema();
    containers2writer(containers, &mut wtr, schema)
}

pub fn unix2docker(
    sock_path: &str,
    timeout_seconds: u64,
    client_version: &ClientVersion,
) -> Result<Docker, io::Error> {
    Docker::connect_with_unix(sock_path, timeout_seconds, client_version).map_err(io::Error::other)
}

pub const DOCKER_UNIX_PATH_DEFAULT: &str = "/var/run/docker.sock";
pub const DOCKER_CON_TIMEOUT_SECONDS_DEFAULT: u64 = 30;
pub const DOCKER_CLIENT_VERSION_DEFAULT: &ClientVersion = bollard::API_DEFAULT_VERSION;
