extern crate netcdf;
extern crate ndarray;
extern crate rayon;

use ndarray::{Array, Array4, ArrayD, ArrayViewD};
use rayon::prelude::*;
use std::{path, str, string, vec};


pub fn destagger_var(
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