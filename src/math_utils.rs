/// A running mean, optionally with weights.
#[derive(Debug, Default)]
pub struct RunningMean {
    summed_val: f64,
    summed_weight: f64,
}

impl RunningMean {
    /// Add a value to the mean, with a weight of 1.
    pub fn add_value<V: Into<f64>>(&mut self, val: V) {
        self.summed_val += val.into();
        self.summed_weight += 1.0;
    }

    /// Add a value to the mean with an arbitrary weight.
    /// Note that the weight could be negative (there is no check).
    pub fn add_weighted_val<V: Into<f64>, W: Into<f64>>(&mut self, val: V, wt: W) {
        let wt = wt.into();
        self.summed_val += val.into() * wt;
        self.summed_weight += wt;
    }

    /// Return the current mean, or `None` if the weight is 0 (meaning no values added).
    pub fn current_mean(&self) -> Option<f64> {
        if self.summed_weight == 0.0 {
            None
        } else {
            Some(self.summed_val / self.summed_weight)
        }
    }
}

/// A running minimum and maximum.
#[derive(Debug, Default)]
pub struct RunningExtrema {
    min: Option<f64>,
    max: Option<f64>,
}

impl RunningExtrema {
    /// Add a value, updating the min and max if needed.
    pub fn add_value<V: Into<f64> + Copy>(&mut self, val: V) {
        self.min = self
            .min
            .and_then(|m| Some(m.min(val.into())))
            .or_else(|| Some(val.into()));
        self.max = self
            .max
            .and_then(|m| Some(m.max(val.into())))
            .or_else(|| Some(val.into()));
    }

    /// Return the current minimum, or `None` if no values were provided.
    pub fn current_min(&self) -> Option<f64> {
        self.min
    }

    /// Return the current maximum, or `None` if no values were provided.
    pub fn current_max(&self) -> Option<f64> {
        self.max
    }
}
