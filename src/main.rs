use std::{borrow::Borrow, collections::HashMap, fs};
use std::fs::ReadDir;
use regex::Regex;

use rayon::prelude::*;
use std::thread;
use std::time::{Duration, Instant};

use std::sync::mpsc::{channel, Receiver, Sender};

extern crate rayon;

// Code Listing 1-1: A sequential word counter
fn sequential_word_counter() -> HashMap<String, i32> {
    // Get all the files in the text_files directory
    let paths = fs::read_dir("text_files").unwrap();

    let mut word_count_map = std::collections::HashMap::<String, i32>::new();
    // Iterate over the files
    for path in paths {
        // Count the number of occurences of "the" in the file
        let file_name = path.as_ref().unwrap().path().display().to_string();
        let contents = fs::read_to_string(path.unwrap().path()).unwrap();
        let mut count = 0;
        let re = Regex::new(r"(?i)\bthe\b").unwrap();
        for _ in re.find_iter(&contents) {
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
    let paths = fs::read_dir("text_files").unwrap();

    paths
        .par_bridge()
        .for_each(|path| {
            // Count the number of occurences of "the" in the file
            let file_name = path.as_ref().unwrap().path().display().to_string();
            let contents = fs::read_to_string(path.unwrap().path()).unwrap();
            let mut count = 0;
            let re = Regex::new(r"(?i)\bthe\b").unwrap();
            // for _ in re.find_iter(&contents) {
            //     count += 1;
            // }

            let words: Vec<_> = contents.split_whitespace().collect();

            let words_with_the: Vec<_> = words
                .par_iter()
                .filter(|word| re.find(word).is_some())
                .collect();

            println!(
                "The file: {} has {:?} occurrences of the word 'the'",
                file_name, words_with_the.len()
            );
        });
}

// Code Listing 1-3: A data-parallel word counter
fn pipeline_parallel_word_counter() {
    struct FileBreakdown
    {
        file_contents: String,
        filename: String,
    }

    struct FileSummary
    {
        count: usize,
        filename: String,
    }

    struct Downloader {
        tx: Sender<FileBreakdown>,
    }

    impl Downloader {
        fn run(&self, files: ReadDir) {
            for file in files {
                let file_name = file.as_ref().unwrap().path().display().to_string();
                let contents = fs::read_to_string(file.unwrap().path()).unwrap();
                let file_pack = FileBreakdown {
                    file_contents: contents,
                    filename: file_name.clone(),
                };
                self.tx.send(file_pack).unwrap();
            }
        }
    }

    struct Processor {
        tx: Sender<FileSummary>,
        rx: Receiver<FileBreakdown>,
    }

    impl Processor {
        fn run(&self) {
            while let Ok(received_file) = self.rx.recv() {
                let re = Regex::new(r"(?i)\bthe\b").unwrap();
                let mut count = 0;
                for _ in re.find_iter(&received_file.file_contents) {
                    count += 1;
                }


                self.tx.send(FileSummary { count, filename: received_file.filename });
            }
        }
    }

    struct Uploader {
        rx: Receiver<FileSummary>,
    }

    impl Uploader {
        fn run(&self) {
            while let Ok(summary) = self.rx.recv() {
                println!(
                    "The file: {} has {} occurrences of the word 'the'",
                    summary.filename, summary.count
                );
            }
        }
    }

    let paths = fs::read_dir("text_files").unwrap();

    let (downloader_tx, processor_rx) = channel();
    let (processor_tx, uploader_rx) = channel();

    let downloader = Downloader { tx: downloader_tx };
    let processor = Processor {
        tx: processor_tx,
        rx: processor_rx,
    };
    let uploader = Uploader { rx: uploader_rx };

    // let files_clone = paths;
    let downloader_thread = thread::spawn(move || downloader.run(paths));
    let processor_thread = thread::spawn(move || processor.run());
    let uploader_thread = thread::spawn(move || uploader.run());
    downloader_thread.join().unwrap();
    processor_thread.join().unwrap();
    uploader_thread.join().unwrap();
}

fn main() {
    // measure_performance(sequential_word_counter, "sequential execution")

//     Measure performance of sequential read
    let start = Instant::now();
    sequential_word_counter();
    let duration = start.elapsed();
    println!("Time elapsed in Sequential execution: {:?}", duration);
//     Measure performance of task parallelism
    let start = Instant::now();
    task_parallel_word_counter();
    let duration = start.elapsed();
    println!("Time elapsed in Parallel execution: {:?}", duration);
//     Measure performance of pipeline parallelism
    let start = Instant::now();
    pipeline_parallel_word_counter();
    let duration = start.elapsed();
    println!("Time elapsed in Pipeline Parallel execution: {:?}", duration);
}
