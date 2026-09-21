//! The Python the walks need, resolved once when a schema is compiled and held by it.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyType};

#[derive(Debug)]
pub struct Values {
    pub datetime: Py<PyType>,
    pub uuid: Py<PyType>,
    pub mapping: Py<PyAny>,
    timedelta: Py<PyAny>,
    timezone: Py<PyAny>,
    epoch: Py<PyAny>,
}

impl Values {
    pub fn new(py: Python<'_>) -> PyResult<Self> {
        let module = py.import("datetime")?;
        let datetime = module.getattr("datetime")?.cast_into::<PyType>()?;
        let utc = module.getattr("UTC")?;
        let named = PyDict::new(py);
        named.set_item("tzinfo", utc)?;
        let epoch = datetime.call((1970, 1, 1), Some(&named))?;
        Ok(Self {
            datetime: datetime.unbind(),
            uuid: py
                .import("uuid")?
                .getattr("UUID")?
                .cast_into::<PyType>()?
                .unbind(),
            mapping: py.import("collections.abc")?.getattr("Mapping")?.unbind(),
            timedelta: module.getattr("timedelta")?.unbind(),
            timezone: module.getattr("timezone")?.unbind(),
            epoch: epoch.unbind(),
        })
    }

    #[must_use]
    pub fn epoch<'py>(&self, py: Python<'py>) -> &Bound<'py, PyAny> {
        self.epoch.bind(py)
    }

    /// `_EPOCH + timedelta(microseconds=micros)`, moved to the zone `offset` seconds from UTC.
    pub fn moment<'py>(
        &self,
        py: Python<'py>,
        micros: i64,
        offset: i32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let named = PyDict::new(py);
        named.set_item("microseconds", micros)?;
        let since = self.timedelta.bind(py).call((), Some(&named))?;
        let zone = self
            .timezone
            .bind(py)
            .call1((self.timedelta.bind(py).call1((0, offset))?,))?;
        self.epoch
            .bind(py)
            .add(since)?
            .call_method1("astimezone", (zone,))
    }
}
