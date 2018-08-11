
extern crate netcdf;
extern crate clap;
extern crate glob;

extern crate ndarray;
use ndarray::ArrayD;
use std::{vec,string,str};
use clap::{Arg, App, SubCommand};
use glob::{glob, Paths};

fn write_variables(ifile: &netcdf::file::File, ofile: &mut netcdf::file::File) -> Result<(), String>{
    
    Ok(())
}


fn write_dimensions(ifile: &netcdf::file::File, ofile: &mut netcdf::file::File) -> Result<(), String> {
    for (name,dim) in &ifile.root.dimensions {
        match ofile.root.add_dimension(&dim.name, dim.len){
            Err(e)=> return Err(e),
            Ok(()) => {}
        };
        }

    Ok(())
}

fn write_global_attributes(ifile: &netcdf::file::File, ofile: &mut netcdf::file::File) -> Result<(), String> {
    for (name,attr) in &ifile.root.attributes{
        let cont = match attr.attrtype {
            NC_CHAR => attr.get_char(false).unwrap()
        };
        match ofile.root.add_attribute(name, cont){
            Err(e) => return Err(e),
            _ => {}
        };
    }
    Ok(())
}


fn process_file(ipath: &str, opath: &str) -> Result<(), String> {
    let mut ifile = match netcdf::open(ipath){
        Ok(ifile)=>ifile,
        Err(e) => panic!("No netcdf file: {:?}", ipath)
    };
    println!("{:?} {:?}", ipath, opath);
    let mut ofile = match netcdf::create(opath){
        Ok(ofile) => ofile,
        Err(e) => panic!("Couldn't create file {:?}", opath)
    };
    match write_global_attributes(&ifile, &mut ofile){
        Ok(n) => println!("Wrote global variables"),
        Err(e) => panic!("Something went wrong!")
    };
    match write_dimensions(&ifile, &mut ofile){
        Ok(n) => println!("Defined dims..."),
        Err(e) => panic!("Something went wrong!")
    };


 //   let latsdata: Vec<f32> = lats.get_float(false).unwrap();
  
    Ok(())
}


fn main() {
    let matches = App::new("Destagger Cosmo Grids")
                          .version("1.0")
                          .author("Kevin K. <kbknapp@gmail.com>")
                          .about("Does awesome things")
                          .arg(Arg::with_name("INPUT")
                               .help("Sets the input file to use")
                               .required(true)
                              .index(1))
                          .arg(Arg::with_name("v")
                               .short("v")
                               .multiple(true)
                               .help("Sets the level of verbosity"))
                          .get_matches();
    let globpattern = matches.value_of("INPUT").unwrap();
    let verbosity = matches.is_present("v");
    let mut pathVec= vec::Vec::new();
    for entry in glob(globpattern).expect("Failed to read glob pattern") {
    match entry {
        Ok(path) => pathVec.push(path),
        Err(e) => println!("{:?}", e),
    }
    };
    println!("{:?}, {:?}", pathVec, verbosity);
    for entry in &pathVec{
        let mut ofile = String::new();
        let file_without_ext = entry.file_stem().unwrap().to_str().unwrap().to_string();
        let parent_path = entry.parent().unwrap().to_str().unwrap().to_string();
        ofile.push_str(parent_path.as_str());
        ofile.push_str("/");
        ofile.push_str(file_without_ext.as_str());
        ofile.push_str("_destag.nc");
        let mut res = process_file(entry.to_str().unwrap(), ofile.as_str()); 
    }

    
}
