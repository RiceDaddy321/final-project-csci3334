use std::{borrow::Borrow, collections::HashMap, fs};

// Code Listing 1-1: A sequential word counter
fn sequential_word_counter() -> HashMap<String, i32> {
    // Get all the files in the text_files directory
    let paths = fs::read_dir("../text_files").unwrap();

    let mut word_count_map = std::collections::HashMap::<String, i32>::new();
    // Iterate over the files
    for path in paths {
        // Count the number of occurences of "the" in the file
        let file_name = path.as_ref().unwrap().path().display().to_string();
        let contents = fs::read_to_string(path.unwrap().path()).unwrap();
        let mut count = 0;
        for _ in contents.matches("the") {
            count += 1;
        }
        println!(
            "The file: {} has {} occurrences of the word 'the'",
            file_name, count
        );

        // Save our result in a map
        word_count_map.insert(file_name, count);
    }

    // return our count map
    return word_count_map;
}

// Code Listing 1-2: A task-parallel word counter
fn task_parallel_word_counter() {
    extern crate rayon;
    use rayon::prelude::*;
    use std::thread;
    use std::time::{Duration, Instant};

    fn download_file(file: &str) -> String {
        thread::sleep(Duration::from_millis(100));
        format!("{} downloaded", file)
    }

    fn resize_image(image: &str) -> String {
        thread::sleep(Duration::from_millis(100));
        format!("{} resized", image)
    }

    let files = vec!["file1.txt", "file2.txt", "file3.txt"];
    let images = vec!["image1.jpg", "image2.jpg", "image3.jpg"];

    // Sequential execution
    let start = Instant::now();
    let downloaded_files: Vec<String> = files.iter().map(|file| download_file(file)).collect();
    let resized_images: Vec<String> = images.iter().map(|image| resize_image(image)).collect();
    let duration = start.elapsed();
    println!("Time elapsed in sequential execution: {:?}", duration);

    // Parallel execution using task parallelism
    let start = Instant::now();
    let (downloaded_files, resized_images): (Vec<String>, Vec<String>) = rayon::join(
        || files.par_iter().map(|file| download_file(file)).collect(),
        || images.par_iter().map(|image| resize_image(image)).collect(),
    );
    let duration = start.elapsed();
    println!("Time elapsed in parallel execution: {:?}", duration);

    println!("Downloaded files: {:?}", downloaded_files);
    println!("Resized images: {:?}", resized_images);
}

// Code Listing 1-3: A data-parallel word counter
fn pipeline_parallel_word_counter() {
    use std::sync::mpsc::{channel, Receiver, Sender};
    use std::thread;
    use std::time::Duration;
    use std::time::Instant;

    enum Message {
        Download(String),
        Process(String),
        Upload(String),
        Exit,
    }

    struct Downloader {
        tx: Sender<Message>,
    }

    impl Downloader {
        fn run(&self, files: &[&str]) {
            for file in files {
                thread::sleep(Duration::from_millis(100)); // Simulate download time
                let downloaded_file = format!("{} downloaded", file);
                self.tx.send(Message::Download(downloaded_file)).unwrap();
            }
            self.tx.send(Message::Exit).unwrap();
        }
    }

    struct Processor {
        tx: Sender<Message>,
        rx: Receiver<Message>,
    }

    impl Processor {
        fn run(&self) {
            loop {
                match self.rx.recv().unwrap() {
                    Message::Download(file) => {
                        thread::sleep(Duration::from_millis(100)); // Simulate processing time
                        let processed_file = format!("{} processed", file);
                        self.tx.send(Message::Process(processed_file)).unwrap();
                    }
                    Message::Exit => {
                        self.tx.send(Message::Exit).unwrap();
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    struct Uploader {
        rx: Receiver<Message>,
    }

    impl Uploader {
        fn run(&self) -> Vec<String> {
            let mut uploaded_files = Vec::new();

            loop {
                match self.rx.recv().unwrap() {
                    Message::Process(file) => {
                        thread::sleep(Duration::from_millis(100)); // Simulate upload time
                        let uploaded_file = format!("{} uploaded", file);
                        uploaded_files.push(uploaded_file);
                    }
                    Message::Exit => {
                        break;
                    }
                    _ => {}
                }
            }

            uploaded_files
        }
    }

    fn sequential(files: &[&str]) -> Vec<String> {
        let mut uploaded_files = Vec::new();

        for file in files {
            thread::sleep(Duration::from_millis(100)); // Simulate download time
            let downloaded_file = format!("{} downloaded", file);

            thread::sleep(Duration::from_millis(100)); // Simulate processing time
            let processed_file = format!("{} processed", downloaded_file);

            thread::sleep(Duration::from_millis(100)); // Simulate upload time
            let uploaded_file = format!("{} uploaded", processed_file);

            uploaded_files.push(uploaded_file);
        }

        uploaded_files
    }

    let files = vec!["file1.txt", "file2.txt", "file3.txt"];

    // Sequential version
    let start = Instant::now();
    let uploaded_files_sequential = sequential(&files);
    let duration_sequential = start.elapsed();
    println!("Sequential duration: {:?}", duration_sequential);
    println!("Sequential uploaded files: {:?}", uploaded_files_sequential);

    // Parallel version
    let start = Instant::now();

    let (downloader_tx, processor_rx) = channel();
    let (processor_tx, uploader_rx) = channel();

    let downloader = Downloader { tx: downloader_tx };
    let processor = Processor {
        tx: processor_tx,
        rx: processor_rx,
    };
    let uploader = Uploader { rx: uploader_rx };

    let files_clone = files.clone();
    let downloader_thread = thread::spawn(move || downloader.run(&files_clone));
    let processor_thread = thread::spawn(move || processor.run());

    let uploaded_files_parallel = uploader.run();

    downloader_thread.join().unwrap();
    processor_thread.join().unwrap();

    let duration_parallel = start.elapsed();
    println!("Parallel duration: {:?}", duration_parallel);
    println!("Parallel uploaded files: {:?}", uploaded_files_parallel);
}

fn main() {
    sequential_word_counter();
    println!("Hello, world!");
}
