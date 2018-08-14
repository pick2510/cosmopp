#![allow(unused_imports)]
extern crate clap;
extern crate glob;
extern crate netcdf;

extern crate ndarray;
use clap::{App, Arg};
use glob::{glob, Paths};
use ndarray::ArrayD;
use std::{str, string, vec};

fn write_attributes(
    ofile: &mut netcdf::file::File,
    attr: &netcdf::attribute::Attribute,
    name: &str,
    var: &netcdf::variable::Variable,
) -> Result<(), String> {
    if name == "_FillValue" {
        return Ok(());
    }
    match attr.attrtype {
        1 => ofile
            .root
            .variables
            .get_mut(&var.name)
            .unwrap()
            .add_attribute(&name, attr.get_byte(false).unwrap_or(1))
            .unwrap(),
        2 => ofile
            .root
            .variables
            .get_mut(&var.name)
            .unwrap()
            .add_attribute(
                &attr.name,
                attr.get_char(false)
                    .unwrap_or("Couldn't read attribute...".to_string()),
            )
            .unwrap(),
        3 => ofile
            .root
            .variables
            .get_mut(&var.name)
            .unwrap()
            .add_attribute(&attr.name, attr.get_short(false).unwrap_or(1))
            .unwrap(),
        4 => ofile
            .root
            .variables
            .get_mut(&var.name)
            .unwrap()
            .add_attribute(&attr.name, attr.get_int(false).unwrap_or(-999))
            .unwrap(),
        5 => ofile
            .root
            .variables
            .get_mut(&var.name)
            .unwrap()
            .add_attribute(&attr.name, attr.get_float(false).unwrap_or(-9e30))
            .unwrap(),
        6 => ofile
            .root
            .variables
            .get_mut(&var.name)
            .unwrap()
            .add_attribute(&attr.name, attr.get_double(false).unwrap_or(-9e30))
            .unwrap(),
        8 => ofile
            .root
            .variables
            .get_mut(&var.name)
            .unwrap()
            .add_attribute(&attr.name, attr.get_ushort(false).unwrap_or(1))
            .unwrap(),
        _ => {}
    }

    Ok(())
}

fn write_variable(
    ofile: &mut netcdf::file::File,
    dimvec: &std::vec::Vec<std::string::String>,
    invar: &netcdf::variable::Variable,
    name: &str,
) -> Result<(), String> {
    match invar.vartype {
        1 => {
            if invar.attributes.contains_key("_FillValue") {
                ofile
                    .root
                    .add_variable_with_fill_value(
                        name,
                        dimvec,
                        &invar.get_byte(false).unwrap(),
                        invar
                            .attributes
                            .get("_FillValue")
                            .unwrap()
                            .get_byte(false)
                            .unwrap_or(-1),
                    )
                    .unwrap();
            } else {
                ofile
                    .root
                    .add_variable(name, dimvec, &invar.get_byte(false).unwrap())
                    .unwrap();
            }
            for (k, attr) in &invar.attributes {
                write_attributes(ofile, attr, &k, &invar).unwrap();
            }
        }
        3 => {
            if invar.attributes.contains_key("_FillValue") {
                ofile
                    .root
                    .add_variable_with_fill_value(
                        name,
                        dimvec,
                        &invar.get_short(false).unwrap(),
                        invar
                            .attributes
                            .get("_FillValue")
                            .unwrap()
                            .get_short(false)
                            .unwrap_or(-999),
                    )
                    .unwrap();
            } else {
                ofile
                    .root
                    .add_variable(name, dimvec, &invar.get_short(false).unwrap())
                    .unwrap();
            }
            for (k, attr) in &invar.attributes {
                write_attributes(ofile, attr, &k, &invar).unwrap();
            }
        }
        4 => {
            if invar.attributes.contains_key("_FillValue") {
                ofile
                    .root
                    .add_variable_with_fill_value(
                        name,
                        dimvec,
                        &invar.get_int(false).unwrap(),
                        invar
                            .attributes
                            .get("_FillValue")
                            .unwrap()
                            .get_int(false)
                            .unwrap(),
                    )
                    .unwrap();
            } else {
                ofile
                    .root
                    .add_variable(name, dimvec, &invar.get_int(false).unwrap())
                    .unwrap();
            }

            for (k, attr) in &invar.attributes {
                write_attributes(ofile, attr, &k, &invar).unwrap();
            }
        }
        5 => {
            if invar.attributes.contains_key("_FillValue") {
                ofile
                    .root
                    .add_variable_with_fill_value(
                        name,
                        dimvec,
                        &invar.get_float(false).unwrap(),
                        invar
                            .attributes
                            .get("_FillValue")
                            .unwrap()
                            .get_float(false)
                            .unwrap_or(-9e-20),
                    )
                    .unwrap();
            } else {
                ofile
                    .root
                    .add_variable(name, dimvec, &invar.get_float(false).unwrap())
                    .unwrap();
            }
            for (k, attr) in &invar.attributes {
                write_attributes(ofile, attr, &k, &invar).unwrap();
            }
        }
        6 => {
            if invar.attributes.contains_key("_FillValue") {
                ofile
                    .root
                    .add_variable_with_fill_value(
                        name,
                        dimvec,
                        &invar.get_double(false).unwrap(),
                        invar
                            .attributes
                            .get("_FillValue")
                            .unwrap()
                            .get_double(false)
                            .unwrap_or(-9e-20),
                    )
                    .unwrap();
            } else {
                ofile
                    .root
                    .add_variable(name, dimvec, &invar.get_double(false).unwrap())
                    .unwrap();
            }
            for (k, attr) in &invar.attributes {
                write_attributes(ofile, attr, &k, &invar).unwrap();
            }
        }

        8 => {
            if invar.attributes.contains_key("_FillValue") {
                ofile
                    .root
                    .add_variable_with_fill_value(
                        name,
                        dimvec,
                        &invar.get_ushort(false).unwrap(),
                        invar
                            .attributes
                            .get("_FillValue")
                            .unwrap()
                            .get_ushort(false)
                            .unwrap_or(999),
                    )
                    .unwrap();
            } else {
                ofile
                    .root
                    .add_variable(name, dimvec, &invar.get_ushort(false).unwrap())
                    .unwrap();
            }

            for (k, attr) in &invar.attributes {
                write_attributes(ofile, attr, &k, &invar).unwrap();
            }
        }
        _ => {}
    }
    Ok(())
}

fn process_vars(ifile: &netcdf::file::File, ofile: &mut netcdf::file::File) -> Result<(), String> {
    for (k, var) in &ifile.root.variables {
        println!("Working on {}", k);
        let mut dimvec = vec![];
        for dim in &var.dimensions {
            dimvec.push(dim.name.clone());
        }
        match write_variable(ofile, &dimvec, var, k) {
            Ok(()) => {}
            Err(n) => return Err(n),
        }
    }
    Ok(())
}

fn write_dimensions(
    ifile: &netcdf::file::File,
    ofile: &mut netcdf::file::File,
) -> Result<(), String> {
    for (name, dim) in &ifile.root.dimensions {
        match ofile.root.add_dimension(&dim.name, dim.len) {
            Err(e) => return Err(e),
            Ok(()) => {}
        };
    }
    Ok(())
}

fn write_global_attributes(
    ifile: &netcdf::file::File,
    ofile: &mut netcdf::file::File,
) -> Result<(), String> {
    for (name, attr) in &ifile.root.attributes {
        match attr.attrtype {
            1 => {
                ofile
                    .root
                    .add_attribute(name, attr.get_byte(false).unwrap())
                    .unwrap();
            }
            2 => {
                ofile
                    .root
                    .add_attribute(name, attr.get_char(false).unwrap())
                    .unwrap();
            }
            3 => {
                ofile
                    .root
                    .add_attribute(name, attr.get_short(false).unwrap())
                    .unwrap();
            }
            4 => {
                ofile
                    .root
                    .add_attribute(name, attr.get_int(false).unwrap())
                    .unwrap();
            }
            5 => {
                ofile
                    .root
                    .add_attribute(name, attr.get_float(false).unwrap())
                    .unwrap();
            }
            6 => {
                ofile
                    .root
                    .add_attribute(name, attr.get_double(false).unwrap())
                    .unwrap();
            }

            _ => continue,
        };
    }
    Ok(())
}

fn process_file(ipath: &str, opath: &str) {
    let ifile = match netcdf::open(ipath) {
        Ok(ifile) => ifile,
        Err(_) => panic!("No netcdf file: {:?}", ipath),
    };
    println!("{:?} {:?}", ipath, opath);
    let mut ofile = match netcdf::create(opath) {
        Ok(ofile) => ofile,
        Err(e) => panic!("Couldn't create file {:?} {:?}", opath, e),
    };
    match write_global_attributes(&ifile, &mut ofile) {
        Ok(()) => println!("Wrote global variables"),
        Err(e) => panic!("Something went wrong: {}!", e),
    };
    match write_dimensions(&ifile, &mut ofile) {
        Ok(()) => println!("Defined dims..."),
        Err(e) => panic!("Something went wrong: {}!", e),
    };
    match process_vars(&ifile, &mut ofile) {
        Ok(()) => println!("Processed vars"),
        Err(e) => panic!("{}", e),
    };
}

fn main() {
    let matches = App::new("Destagger Cosmo Grids")
        .version("1.0")
        .author("Dominik Strebel <dominik.strebel@empa.ch>")
        .about("Does awesome things\nDestagger COSMO grids")
        .arg(
            Arg::with_name("INPUT")
                .help("Sets the input file to use")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::with_name("v")
                .short("v")
                .multiple(true)
                .help("Sets the level of verbosity"),
        )
        .get_matches();
    let globpattern = matches.value_of("INPUT").unwrap();
    let verbosity = matches.is_present("v");
    let mut pathVec = vec::Vec::new();
    for entry in glob(globpattern).unwrap() {
        match entry {
            Ok(path) => pathVec.push(path),
            Err(e) => println!("{:?}", e),
        }
    }
    println!("{:?}, {:?}", pathVec, verbosity);
    for entry in &pathVec {
        let tmpfile = entry.to_str().unwrap();
        if entry.extension().unwrap() != "nc" {
            println!("netCDF file needed, continue");
            continue;
        }
        let ofile = tmpfile.replace(".nc", "_destagger.nc");
        process_file(entry.to_str().unwrap(), &ofile);
    }
}
