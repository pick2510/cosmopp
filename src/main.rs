#![allow(unused_imports)]
extern crate clap;
extern crate glob;
extern crate netcdf;
extern crate rayon;
#[macro_use]
extern crate ndarray;
use clap::{App, Arg};
use glob::{glob, Paths};
use ndarray::{Array, Array4, ArrayD, ArrayViewD};
use rayon::prelude::*;
use std::{path, str, string, vec};

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
            .add_attribute(&name, attr.get_byte(false).unwrap_or(1))?,
        2 => ofile
            .root
            .variables
            .get_mut(&var.name)
            .unwrap()
            .add_attribute(
                &attr.name,
                attr.get_char(false)
                    .unwrap_or("Couldn't read attribute...".to_string()),
            )?,
        3 => ofile
            .root
            .variables
            .get_mut(&var.name)
            .unwrap()
            .add_attribute(&attr.name, attr.get_short(false).unwrap_or(1))?,
        4 => ofile
            .root
            .variables
            .get_mut(&var.name)
            .unwrap()
            .add_attribute(&attr.name, attr.get_int(false).unwrap_or(-999))?,
        5 => ofile
            .root
            .variables
            .get_mut(&var.name)
            .unwrap()
            .add_attribute(&attr.name, attr.get_float(false).unwrap_or(-9e30))?,
        6 => ofile
            .root
            .variables
            .get_mut(&var.name)
            .unwrap()
            .add_attribute(&attr.name, attr.get_double(false).unwrap_or(-9e30))?,
        8 => ofile
            .root
            .variables
            .get_mut(&var.name)
            .unwrap()
            .add_attribute(&attr.name, attr.get_ushort(false).unwrap_or(1))?,
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
                    .add_variable(name, dimvec, &invar.get_byte(false).unwrap())?
            }
            for (k, attr) in &invar.attributes {
                write_attributes(ofile, attr, &k, &invar)?
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
                    .add_variable(name, dimvec, &invar.get_short(false).unwrap())?
            }
            for (k, attr) in &invar.attributes {
                write_attributes(ofile, attr, &k, &invar)?
            }
        }
        4 => {
            if invar.attributes.contains_key("_FillValue") {
                ofile.root.add_variable_with_fill_value(
                    name,
                    dimvec,
                    &invar.get_int(false).unwrap(),
                    invar
                        .attributes
                        .get("_FillValue")
                        .unwrap()
                        .get_int(false)
                        .unwrap_or(-999),
                )?
            } else {
                ofile
                    .root
                    .add_variable(name, dimvec, &invar.get_int(false).unwrap())?
            }

            for (k, attr) in &invar.attributes {
                write_attributes(ofile, attr, &k, &invar).unwrap();
            }
        }
        5 => {
            if invar.attributes.contains_key("_FillValue") {
                ofile.root.add_variable_with_fill_value(
                    name,
                    dimvec,
                    &invar.get_float(false).unwrap(),
                    invar
                        .attributes
                        .get("_FillValue")
                        .unwrap()
                        .get_float(false)
                        .unwrap_or(-9e-20),
                )?
            } else {
                ofile
                    .root
                    .add_variable(name, dimvec, &invar.get_float(false).unwrap())
                    .unwrap();
            }
            for (k, attr) in &invar.attributes {
                write_attributes(ofile, attr, &k, &invar)?
            }
        }
        6 => {
            if invar.attributes.contains_key("_FillValue") {
                ofile.root.add_variable_with_fill_value(
                    name,
                    dimvec,
                    &invar.get_double(false).unwrap(),
                    invar
                        .attributes
                        .get("_FillValue")
                        .unwrap()
                        .get_double(false)
                        .unwrap_or(-9e-20),
                )?
            } else {
                ofile
                    .root
                    .add_variable(name, dimvec, &invar.get_double(false).unwrap())?;
            }
            for (k, attr) in &invar.attributes {
                write_attributes(ofile, attr, &k, &invar)?
            }
        }

        8 => {
            if invar.attributes.contains_key("_FillValue") {
                ofile.root.add_variable_with_fill_value(
                    name,
                    dimvec,
                    &invar.get_ushort(false).unwrap(),
                    invar
                        .attributes
                        .get("_FillValue")
                        .unwrap()
                        .get_ushort(false)
                        .unwrap_or(999),
                )?;
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

fn destagger_var(
    ifile: &netcdf::file::File,
    ofile: &mut netcdf::file::File,
    var: &netcdf::variable::Variable,
    verbosity: &bool,
) -> Result<(), String> {
    let dtime = ifile.root.dimensions.get("time").unwrap().len;
    let dlat = ifile.root.dimensions.get("rlat").unwrap().len;
    let dlon = ifile.root.dimensions.get("rlon").unwrap().len;
    let dlevs = ifile.root.dimensions.get("level").unwrap().len;
    if var.name == "U" {
        if *verbosity {
            println!("Destaggering U...");
        }
        let dsrlon = ifile.root.dimensions.get("srlon").unwrap().len;
        let udims = vec![
            "time".to_owned(),
            "level".to_owned(),
            "rlat".to_owned(),
            "rlon".to_owned(),
        ];
        let mut res_array: Array4<f64> =
            ndarray::Array::zeros((dtime as usize, dlevs as usize, dlat as usize, dlon as usize));
        res_array.fill(-1e-20);
        for ts in 0..dtime {
            if *verbosity {
                println!("Timestep: {}", ts);
            }
            for lev in 0..dlevs {
                if *verbosity {
                    println!("Level: {}", lev);
                }
                let values: ArrayD<f64> =
                    var.array_at(
                        &[ts as usize, lev as usize, 0, 0],
                        &[1, 1, dlat as usize, dsrlon as usize],
                    ).unwrap();
                let slice1 = values
                    .clone()
                    .slice_move(s![0, 0, .., 0..(dsrlon - 1) as usize]);
                let slice2 = values.slice_move(s![0, 0, .., 1..dsrlon as usize]);
                let destag = (slice1 + slice2) * 0.5;
                res_array
                    .slice_mut(s![ts as usize, lev as usize, .., 1..])
                    .assign(&destag);
            }
        }
        ofile.root.add_variable_with_fill_value(
            "U_destag",
            &udims,
            &res_array.into_raw_vec(),
            1e-20,
        )?
    } else if var.name == "V" {
        if *verbosity {
            println!("Destaggering V...");
        }
        let dsrlat = ifile.root.dimensions.get("srlat").unwrap().len;
        //println!("{} {} {}", dtime, dlat, dlon);
        let vdims = vec![
            "time".to_owned(),
            "level".to_owned(),
            "rlat".to_owned(),
            "rlon".to_owned(),
        ];
        let mut res_array: Array4<f64> =
            ndarray::Array::zeros((dtime as usize, dlevs as usize, dlat as usize, dlon as usize));
        res_array.fill(-1e-20);
        for ts in 0..dtime {
            if *verbosity {
                println!("Timestep: {}", ts);
            }
            for lev in 0..dlevs {
                if *verbosity {
                    println!("Level: {}", lev);
                }
                let values: ArrayD<f64> =
                    var.array_at(
                        &[ts as usize, lev as usize, 0, 0],
                        &[1, 1, dsrlat as usize, dlon as usize],
                    ).unwrap();
                let slice1 = values
                    .clone()
                    .slice_move(s![0, 0, 0..(dsrlat - 1) as usize, ..]);
                let slice2 = values.slice_move(s![0, 0, 1..dsrlat as usize, ..]);
                let destag = (slice1 + slice2) * 0.5;
                res_array
                    .slice_mut(s![ts as usize, lev as usize, 1.., ..])
                    .assign(&destag);
            }
        }
        ofile.root.add_variable_with_fill_value(
            "V_destag",
            &vdims,
            &res_array.into_raw_vec(),
            -1e-20,
        )?;
    }
    Ok(())
}

fn calc_wind_mag(ofile: &mut netcdf::file::File, verbosity: &bool) -> Result<(), String> {
    if *verbosity {
        println!("Calculate magnitude of windspeed on each level.. ");
    }
    let dtime = ofile.root.dimensions.get("time").unwrap().len;
    let dlat = ofile.root.dimensions.get("rlat").unwrap().len;
    let dlon = ofile.root.dimensions.get("rlon").unwrap().len;
    let dlevs = ofile.root.dimensions.get("level").unwrap().len;
    let magdims = vec![
        "time".to_owned(),
        "level".to_owned(),
        "rlat".to_owned(),
        "rlon".to_owned(),
    ];
    let mut res_array: Array4<f64> =
        ndarray::Array::zeros((dtime as usize, dlevs as usize, dlat as usize, dlon as usize));
    res_array.fill(-1e-20);
    {
        let u_destag = ofile.root.variables.get("U_destag").unwrap();
        let v_destag = ofile.root.variables.get("V_destag").unwrap();

        for ts in 0..dtime {
            if *verbosity {
                println!("Timestep: {}", ts);
            }
            for lev in 0..dlevs {
                if *verbosity {
                    println!("Level: {}", lev);
                }
                let u_values: ArrayD<f64> = u_destag
                    .array_at(
                        &[ts as usize, lev as usize, 0, 0],
                        &[1, 1, dlat as usize, dlon as usize],
                    )
                    .unwrap();
                let v_values: ArrayD<f64> = v_destag
                    .array_at(
                        &[ts as usize, lev as usize, 0, 0],
                        &[1, 1, dlat as usize, dlon as usize],
                    )
                    .unwrap();
                let uslice = u_values.slice_move(s![0, 0, .., ..]);
                let vslice = v_values.slice_move(s![0, 0, .., ..]);
                let w_mag =
                    (uslice.mapv(|x| x.powi(2)) + vslice.mapv(|x| x.powi(2))).mapv(f64::sqrt);
                res_array
                    .slice_mut(s![ts as usize, lev as usize, .., ..])
                    .assign(&w_mag);
            }
        }
    }

    ofile
        .root
        .add_variable_with_fill_value("Wind_Mag", &magdims, &res_array.into_raw_vec(), -1e-20)?;
    Ok(())
}

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

fn write_dimensions(
    ifile: &netcdf::file::File,
    ofile: &mut netcdf::file::File,
) -> Result<(), String> {
    for (name, dim) in &ifile.root.dimensions {
        match ofile.root.add_dimension(&name, dim.len) {
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
