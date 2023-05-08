use std::{borrow::Borrow, collections::HashMap, fs};
use std::time::Instant;
use regex::Regex;

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
fn rayon_iterators_real_power() {
    // https://docs.rs/rayon/latest/rayon/
    extern crate rayon; // 1.5.3
    use rayon::prelude::*; // 1.5.3

    let wiki_txt = " Parallel computing is a type of computation in which many calculations or processes are carried out simultaneously.
    Large problems can often be divided into smaller ones, which can then be solved at the same time.
    There are several different forms of parallel computing: bit-level, instruction-level, data, and task parallelism.
    Parallelism has long been employed in high-performance computing, but has gained broader interest due to the physical
    constraints preventing frequency scaling.Parallel computing is closely related to concurrent computing—
    they are frequently used together, and often conflated, though the two are distinct:
    it is possible to have parallelism without concurrency, and concurrency without parallelism
    (such as multitasking by time-sharing on a single-core CPU).
    In parallel computing, a computational task is typically broken down into several, often many,
    very similar sub-tasks that can be processed independently and whose results are combined afterwards, upon completion.
    In contrast, in concurrent computing, the various processes often do not address related tasks;
    when they do, as is typical in distributed computing, the separate tasks may have a varied nature and often require some
    inter-process communication during execution.";

    let words: Vec<_> = wiki_txt.split_whitespace().collect();

    // par_iter() -> parallel iterator

    words.par_iter().for_each(|val| println!("{}", val));

    // par_iterator can do everything as regular iterator, but can does it
    // in parallel

    let words_with_p: Vec<_> = words
        .par_iter()
        .filter(|val| val.find('p').is_some()) // of course notice the closure FN, which borrows for reading only
        .collect();

    println!("All words with letter p: {:?}", words_with_p);
}

fn task_parallel_word_counter() {
    extern crate rayon;
    use rayon::prelude::*;
    use std::thread;
    use std::time::{Duration, Instant};

    let paths = fs::read_dir("text_files").unwrap();
    // paths.par_bridge().for_each(|val| {})
    // words.par_iter().for_each(|val| println!("{}", val));
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
    // let words_with_p: Vec<_> = words
    //     .par_iter()
    //     .filter(|val| val.find('p').is_some()) // of course notice the closure FN, which borrows for reading only
    //     .collect();

    // println!("All words with letter p: {:?}", words_with_the);
}

// Code Listing 1-3: A data-parallel word counter
fn pipeline_parallel_word_counter() {
    use std::fs;
    use std::sync::mpsc::{channel, Receiver, Sender};
    use std::thread;
    use std::time::Duration;
    use std::time::Instant;

    fn count_the(rx: Receiver<String>, tx: Sender<usize>) {
        let mut count = 0;
        for line in rx.iter() {
            let words: Vec<&str> = line.split_whitespace().collect();
            for word in words {
                if word.to_lowercase() == "the" {
                    count += 1;
                }
            }
        }
        tx.send(count).unwrap();
    }

    let paths = fs::read_dir("text_files").unwrap();

    let (tx1, rx1): (Sender<String>, Receiver<String>) = channel();
    let (tx2, rx2): (Sender<usize>, Receiver<usize>) = channel();

    let mut thread_handles = Vec::new();

    // Create a thread for each file in the directory
    for path in paths {
        let path = path.unwrap().path();
        if let Some(file_name) = path.file_name() {
            let tx1 = tx1.clone();
            let tx2 = tx2.clone();
            let file_name = file_name.to_str().unwrap().to_string();
            let thread_handle = thread::spawn(move || {
                let contents = fs::read_to_string(&path).unwrap();
                for line in contents.lines() {
                    tx1.send(line.to_string()).unwrap();
                }
                tx1.send("".to_string()).unwrap(); // Signal end of file
                let count = rx2.recv().unwrap();
                println!(
                    "The file: {} has {} occurrences of the word 'the'",
                    file_name, count
                );
                tx2.send(count).unwrap();
            });
            thread_handles.push(thread_handle);
        }
    }

    // Create a thread to count the word "the" and its variations
    let (tx3, rx3): (Sender<usize>, Receiver<usize>) = channel();
    thread::spawn(move || {
        let mut count = 0;
        loop {
            let line = rx1.recv().unwrap();
            if line.is_empty() {
                tx3.send(count).unwrap();
                count = 0;
            } else {
                let words: Vec<&str> = line.split_whitespace().collect();
                for word in words {
                    if word.to_lowercase() == "the" {
                        count += 1;
                    }
                }
            }
        }
    });

    // Wait for all threads to complete
    for handle in thread_handles {
        handle.join().unwrap();
    }

    // Print the total number of occurrences of the word "the"
    let start = Instant::now();
    let mut count = 0;
    for c in rx3 {
        count += c;
        println!("{} occurrences of 'the' found so far.", count);
    }
    let duration = start.elapsed();
    println!("Total time elapsed: {:?}", duration);
}

fn measure_performance<F>(function: F, name_of_function: &str)
    where
        F: Fn(),
{
    let start = Instant::now();
    function();
    let duration = start.elapsed();
    println!("Time elapsed in {}: {:?}", name_of_function, duration);
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
