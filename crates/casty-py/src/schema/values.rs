//! The Python the walks need, resolved once when a schema is compiled and held by it.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyType};

/// `date(1970, 1, 1).toordinal()`: a `date` counts its days from the epoch a `datetime` counts its microseconds from.
const EPOCH_ORDINAL: i64 = 719_163;

#[derive(Debug)]
pub struct Values {
    pub datetime: Py<PyType>,
    pub date: Py<PyType>,
    pub time: Py<PyType>,
    pub timedelta: Py<PyType>,
    pub decimal: Py<PyType>,
    pub uuid: Py<PyType>,
    pub mapping: Py<PyAny>,
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
            date: module.getattr("date")?.cast_into::<PyType>()?.unbind(),
            time: module.getattr("time")?.cast_into::<PyType>()?.unbind(),
            timedelta: module.getattr("timedelta")?.cast_into::<PyType>()?.unbind(),
            decimal: py
                .import("decimal")?
                .getattr("Decimal")?
                .cast_into::<PyType>()?
                .unbind(),
            uuid: py
                .import("uuid")?
                .getattr("UUID")?
                .cast_into::<PyType>()?
                .unbind(),
            mapping: py.import("collections.abc")?.getattr("Mapping")?.unbind(),
            timezone: module.getattr("timezone")?.unbind(),
            epoch: epoch.unbind(),
        })
    }

    #[must_use]
    pub fn epoch<'py>(&self, py: Python<'py>) -> &Bound<'py, PyAny> {
        self.epoch.bind(py)
    }

    /// `timedelta(microseconds=micros)`.
    pub fn duration<'py>(&self, py: Python<'py>, micros: i64) -> PyResult<Bound<'py, PyAny>> {
        let named = PyDict::new(py);
        named.set_item("microseconds", micros)?;
        self.timedelta.bind(py).call((), Some(&named))
    }

    /// `_EPOCH + timedelta(microseconds=micros)`, moved to the zone `offset` seconds from UTC.
    pub fn moment<'py>(
        &self,
        py: Python<'py>,
        micros: i64,
        offset: i32,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.epoch
            .bind(py)
            .add(self.duration(py, micros)?)?
            .call_method1("astimezone", (self.zone(py, offset)?,))
    }

    /// The `time` `micros` after midnight, in the zone `offset` seconds from UTC.
    pub fn clock<'py>(
        &self,
        py: Python<'py>,
        micros: i64,
        offset: i32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let named = PyDict::new(py);
        named.set_item("tzinfo", self.zone(py, offset)?)?;
        let seconds = micros.div_euclid(1_000_000);
        let parts = (
            seconds / 3_600,
            seconds / 60 % 60,
            seconds % 60,
            micros.rem_euclid(1_000_000),
        );
        self.time.bind(py).call(parts, Some(&named))
    }

    /// Whether `value` is a `date` and not a `datetime`, which is one too and would lose its time as a `date`.
    pub fn is_date(&self, value: &Bound<'_, PyAny>) -> PyResult<bool> {
        let py = value.py();
        Ok(value.is_instance(self.date.bind(py))? && !value.is_instance(self.datetime.bind(py))?)
    }

    /// The `date` `days` after the epoch.
    pub fn day<'py>(&self, py: Python<'py>, days: i64) -> PyResult<Bound<'py, PyAny>> {
        let ordinal = days.saturating_add(EPOCH_ORDINAL);
        self.date.bind(py).call_method1("fromordinal", (ordinal,))
    }

    /// `timezone(timedelta(seconds=offset))`.
    fn zone<'py>(&self, py: Python<'py>, offset: i32) -> PyResult<Bound<'py, PyAny>> {
        self.timezone
            .bind(py)
            .call1((self.timedelta.bind(py).call1((0, offset))?,))
    }
}

/// The days from the epoch to the `date` `value`.
pub fn days(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    Ok(value.call_method0("toordinal")?.extract::<i64>()? - EPOCH_ORDINAL)
}
