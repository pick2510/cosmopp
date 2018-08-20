extern crate netcdf;
extern crate ndarray;
extern crate rayon;

use ndarray::{Array, Array4, ArrayD, ArrayViewD};
use rayon::prelude::*;
use std::{path, str, string, vec};


pub fn calc_wind_mag(ofile: &mut netcdf::file::File, verbosity: &bool) -> Result<(), String> {
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
