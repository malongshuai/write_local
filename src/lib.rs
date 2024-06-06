use flume::{bounded, Receiver, Sender};
use fs_err::{write, OpenOptions};
use std::{collections::HashMap, io::Write as _, path::PathBuf};
use tracing::{error, info, warn};

/// 集中(收集)所有要写入本地的数据，要写入同个文件的多批次数据尽可能地被合并，减少写入本地文件的次数
///
/// 需注意，每次写入文件时，都会重新打开一次文件(写完自动关闭)
///
/// ```
/// // 初始化
/// let local_writer = WriteLocal::init();
///
/// // 追加写入到文件
/// let dest_file = PathBuf::from_str("/tmp/a.log").unwrap();
/// let data = "helloworld".as_bytes().to_vec();
/// local_writer.write(dest_file, WriteData::Append(data));
/// ```
#[derive(Clone)]
pub struct WriteLocal {
    tx: Sender<(PathBuf, WriteData)>,
}

impl WriteLocal {
    /// 初始化
    pub fn init() -> Self {
        let (tx, rx) = bounded::<(PathBuf, WriteData)>(1000);

        std::thread::spawn(move || {
            write_to_local(rx);
        });

        Self { tx }
    }

    /// 发送要写到本地文件的路径和数据
    pub fn write(&self, dest_file: PathBuf, data: WriteData) {
        let _ = self.tx.send((dest_file, data));
    }
}

/// 待写入本地的(字节)数据是要追加的还是截断覆盖原有数据的
pub enum WriteData {
    Append(Vec<u8>),
    Override(Vec<u8>),
}

impl WriteData {
    /// 覆盖类型的数据，直接替换，追加类型的数据，直接追加在尾部
    fn write(&mut self, data: Vec<u8>) {
        match self {
            WriteData::Append(local) => local.extend(data),
            WriteData::Override(local) => *local = data,
        }
    }

    /// 数据已经写入本地之后，清空这些已写数据
    fn clear(&mut self) {
        match self {
            WriteData::Append(local) => local.clear(),
            WriteData::Override(local) => local.clear(),
        }
    }

    #[allow(dead_code)]
    fn len(&self) -> usize {
        match self {
            WriteData::Append(local) => local.len(),
            WriteData::Override(local) => local.len(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            WriteData::Append(local) => local.is_empty(),
            WriteData::Override(local) => local.is_empty(),
        }
    }
}

fn write_to_local(rx: Receiver<(PathBuf, WriteData)>) {
    let mut cached: HashMap<PathBuf, WriteData> = HashMap::with_capacity(10);
    let mut tmp = Vec::with_capacity(10);
    loop {
        // 先用recv阻塞接收消息，然后通过rx.drain()一次性读取channel中的所有消息
        match rx.recv() {
            Ok(data) => tmp.push(data),
            Err(e) => {
                warn!("write local channel sender closed: {e}");
                break;
            }
        };

        // 稍稍小睡一会会，等待更多数据的到来
        std::thread::sleep(std::time::Duration::from_millis(100));

        tmp.extend(rx.drain()); // flume的Receiver::drain()是不阻塞的，总是立即返回

        // 合并要写的内容
        for (f, d) in tmp.drain(..) {
            // 接收到了空数据想要写入
            if d.is_empty() {
                warn!("recv empty data want write to {:?}, skip", f.as_os_str());
                continue;
            }
            match d {
                WriteData::Append(data) => cached
                    .entry(f)
                    .or_insert(WriteData::Append(Vec::new()))
                    .write(data),
                WriteData::Override(data) => {
                    cached
                        .entry(f)
                        .or_insert(WriteData::Override(Vec::new()))
                        .write(data);
                }
            }
        }

        // 尽管data部分在每次写入完成之后都会被清空，
        // 但由于是iter_mut()而不是直接删除HashMap中的所有元素，所以总是存在元素而进入for的迭代，
        // 因此loop的开头部分需通过阻塞的方式等待可写数据
        for (f, data) in cached.iter_mut() {
            // 某个文件接收到数据后，其它缓存的路径下可能没有要写的数据，因此跳过空的
            if data.is_empty() {
                continue;
            }
            match data {
                WriteData::Override(data) => match write(f, &data) {
                    Err(e) => error!("{e}"),
                    Ok(_) => info!("override {} bytes to {:?}", data.len(), f.as_os_str()),
                },
                WriteData::Append(data) => {
                    let file = OpenOptions::new().append(true).create(true).open(f);
                    match file {
                        Err(e) => error!("{e}"),
                        Ok(mut file) => match file.write(data) {
                            Ok(n) => info!("append {n} bytes to {:?}", f.as_os_str()),
                            Err(e) => error!("{e}"),
                        },
                    }
                }
            }
            // 本次数据写完之后清空
            data.clear();
        }
    }
}
