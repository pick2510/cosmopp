extern crate netcdf;
extern crate ndarray;
extern crate rayon;


use ndarray::{Array, Array4, ArrayD, ArrayViewD};
use rayon::prelude::*;
use std::{path, str, string, vec};


pub fn write_attributes(
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

pub fn write_variable(
    ofile: &mut netcdf::file::File,
    dimvec: &vec::Vec<string::String>,
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


pub fn write_global_attributes(
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


pub fn write_dimensions(
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
