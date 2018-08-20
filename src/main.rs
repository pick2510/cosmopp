#![allow(unused_imports)]
mod destagger;
mod windmag;
mod write;

extern crate clap;
extern crate netcdf;
extern crate rayon;
#[macro_use]
extern crate ndarray;
use clap::{App, Arg};
use ndarray::{Array, Array4, ArrayD, ArrayViewD};
use rayon::prelude::*;
use std::{path, str, string, vec};
use destagger::destagger_var;
use write::{write_attributes, write_dimensions, write_global_attributes, write_variable};
use windmag::calc_wind_mag;




fn process_vars(
    ifile: &netcdf::file::File,
    ofile: &mut netcdf::file::File,
    verbosity: &bool,
) -> Result<(), String> {
    for (k, var) in &ifile.root.variables {
        if *verbosity {
            println!("Working on {}", k);
        }
        let mut dimvec = vec![];
        for dim in &var.dimensions {
            dimvec.push(dim.name.clone());
        }
        if k == "U" || k == "V" {
            destagger_var(ifile, ofile, var, verbosity)?
        } else {
            write_variable(ofile, &dimvec, var, k)?
        }
    }
    Ok(())
}


fn process_file(ipath: &str, opath: &str, verbosity: &bool, w_mag: &bool) {
    let ifile = match netcdf::open(ipath) {
        Ok(ifile) => ifile,
        Err(_) => panic!("No netcdf file: {:?}", ipath),
    };
    if *verbosity {
        println!("{:?} {:?}", ipath, opath);
    }
    let mut ofile = match netcdf::create(opath) {
        Ok(ofile) => ofile,
        Err(e) => panic!("Couldn't create file {:?} {:?}", opath, e),
    };
    match write_global_attributes(&ifile, &mut ofile) {
        Ok(()) => {
            if *verbosity {
                println!("Wrote global variables")
            }
        }
        Err(e) => panic!("Something went wrong: {}!", e),
    };
    match write_dimensions(&ifile, &mut ofile) {
        Ok(()) => {
            if *verbosity {
                println!("Defined dims...")
            }
        }
        Err(e) => panic!("Something went wrong: {}!", e),
    };
    match process_vars(&ifile, &mut ofile, &verbosity) {
        Ok(()) => {}
        Err(e) => panic!("{}", e),
    };
    if *w_mag && ofile.root.variables.contains_key("U_destag") {
        match calc_wind_mag(&mut ofile, &verbosity) {
            Ok(()) => println!("Finished {}", ifile.name),
            Err(e) => panic!("{}", e),
        }
    }
}

fn worker(entry: &str, verbosity: &bool, w_mag: &bool) {
    let tmpfile = &entry;
    let tmppath = path::Path::new(entry);
    if tmppath.extension().unwrap() != "nc" {
        println!("netCDF file needed, continue");
        return;
    }
    let ofile = tmpfile.replace(".nc", "_destagger.nc");
    process_file(&entry, &ofile, &verbosity, &w_mag);
}

fn main() {
    let matches = App::new("Destagger cosmo netCDF files")
        .version("0.2")
        .author("Dominik Strebel <dominik.strebel@empa.ch>")
        .about("Des awesome things\nDestagger COSMO grids")
        .arg(
            Arg::with_name("p")
                .short("p")
                .multiple(false)
                .help("Parallel version"),
        )
        .arg(
            Arg::with_name("m")
                .short("m")
                .multiple(false)
                .help("Calc magnitude of windspeed"),
        )
        .arg(
            Arg::with_name("v")
                .short("v")
                .multiple(true)
                .help("Sets the level of verbosity"),
        )
        .arg(
            Arg::with_name("INPUT")
                .help("Sets the input file to use")
                .required(true)
                .multiple(true)
                .index(1),
        )
        .get_matches();
    let verbosity = matches.is_present("v");
    let calc_magnitude = matches.is_present("m");
    let mut pathVec = vec::Vec::new();
    if let Some(in_v) = matches.values_of("INPUT") {
        for in_file in in_v {
            pathVec.push(in_file);
        }
    }

    if matches.is_present("p") {
        pathVec
            .par_iter()
            .for_each(|entry| worker(&entry, &verbosity, &calc_magnitude));
    } else {
        pathVec
            .iter()
            .for_each(|entry| worker(&entry, &verbosity, &calc_magnitude));
    }
}
