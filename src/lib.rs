use pyo3::prelude::*;
use std::collections::HashMap;

#[pyclass]
struct FileStats {
    size: f32,
    hit: i32,
    miss: i32,
    last_request: i32,
    data_type: i32,
}

#[pymethods]
impl FileStats {
    #[new]
    fn new(size: f32, data_type: i32) -> Self {
        FileStats {
            size: size,
            hit: 0,
            miss: 0,
            last_request: -1,
            data_type: data_type,
        }
    }

    fn update(&mut self, hit: bool) {
        if hit {
            self.hit += 1
        } else {
            self.miss += 1
        }
    }

    #[getter]
    fn tot_requests(&self) -> PyResult<i32> {
        Ok(self.hit + self.miss)
    }

    #[getter]
    fn hit(&self) -> PyResult<i32> {
        Ok(self.hit)
    }

    #[getter]
    fn miss(&self) -> PyResult<i32> {
        Ok(self.miss)
    }

    #[getter(datatype)]
    fn get_data_type(&self) -> PyResult<i32> {
        Ok(self.data_type)
    }

    #[getter(last_request)]
    fn get_last_request(&self) -> PyResult<i32> {
        Ok(self.last_request)
    }

    #[getter]
    fn size(&self) -> PyResult<f32> {
        Ok(self.size)
    }

    #[setter(last_request)]
    fn set_last_request(&mut self, request: i32) -> PyResult<()> {
        self.last_request = request;
        Ok(())
    }

    #[getter(values)]
    fn get_values(&self) -> (f32, i32, i32, i32) {
        return (
            self.size,
            self.hit + self.miss,
            self.last_request,
            self.data_type,
        );
    }
}

#[pyclass]
struct Stats {
    files: HashMap<i32, Py<FileStats>>,
    last_file: i32,
}

#[pymethods]
impl Stats {
    #[new]
    fn new() -> Self {
        Stats {
            files: HashMap::new(),
            last_file: -1,
        }
    }

    fn get_or_set(
        &mut self,
        filename: i32,
        size: f32,
        data_type: i32,
        request: i32,
    ) -> &Py<FileStats> {
        self.last_file = filename;
        let gil = Python::acquire_gil();
        if !self.files.contains_key(&self.last_file) {
            // println!("NEW");
            let py = gil.python();
            let new_file_stats = Py::new(
                py,
                FileStats {
                    size: size,
                    hit: 0,
                    miss: 0,
                    last_request: request,
                    data_type: data_type,
                },
            )
            .unwrap();
            // println!("[{}] -> {}", request, new_file_stats.as_ref(gil.python()).borrow().last_request);
            self.files.insert(self.last_file, new_file_stats);
            return &self.files.get(&self.last_file).unwrap();
        } else {
            // println!("UPDATE");
            let stats = self.files.get(&self.last_file).unwrap();
            let mut cell = stats.as_ref(gil.python()).borrow_mut();
            cell.size = size;
            // stats.last_request = request;
            // println!("{} {}", stats.last_request, request);
            return stats;
        }
    }

    fn update(&mut self, hit: bool) {
        match self.files.get_mut(&self.last_file) {
            Some(stats) => {
                let gil = Python::acquire_gil();
                let mut cell = stats.as_ref(gil.python()).borrow_mut();
                cell.update(hit);
            }
            None => (),
        };
    }

    fn get_values(&mut self) -> (f32, i32, i32, i32) {
        let mut tuple = (-1.0, -1, -1, -1);
        match self.files.get_mut(&self.last_file) {
            Some(stats) => {
                let gil = Python::acquire_gil();
                let cell = stats.as_ref(gil.python()).borrow();
                tuple.0 = cell.size;
                tuple.1 = cell.hit + cell.miss;
                tuple.2 = cell.last_request;
                tuple.3 = cell.data_type;
            }
            None => (),
        };
        return tuple;
    }
}

#[pymodule]
// A Python module implemented in Rust.
fn stats_mod(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<FileStats>()?;
    m.add_class::<Stats>()?;
    Ok(())
}
