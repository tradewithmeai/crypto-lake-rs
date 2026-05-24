/// Pure indicator math — ported from static/js/dashboard.js plus MACD.

pub fn calc_sma(values: &[f64], period: usize) -> Vec<Option<f64>> {
    let n = values.len();
    let mut result = vec![None; n];
    if period == 0 || n < period {
        return result;
    }
    let mut sum: f64 = values[..period].iter().sum();
    result[period - 1] = Some(sum / period as f64);
    for i in period..n {
        sum += values[i] - values[i - period];
        result[i] = Some(sum / period as f64);
    }
    result
}

pub fn calc_ema(values: &[f64], period: usize) -> Vec<Option<f64>> {
    let n = values.len();
    let mut result = vec![None; n];
    if period == 0 || n < period {
        return result;
    }
    let alpha = 2.0 / (period as f64 + 1.0);
    let seed: f64 = values[..period].iter().sum::<f64>() / period as f64;
    result[period - 1] = Some(seed);
    let mut ema = seed;
    for i in period..n {
        ema = alpha * values[i] + (1.0 - alpha) * ema;
        result[i] = Some(ema);
    }
    result
}

pub fn calc_rsi(values: &[f64], period: usize) -> Vec<Option<f64>> {
    let n = values.len();
    let mut result = vec![None; n];
    if n < period + 1 {
        return result;
    }
    let mut gain_sum = 0.0f64;
    let mut loss_sum = 0.0f64;
    for i in 1..=period {
        let diff = values[i] - values[i - 1];
        if diff > 0.0 {
            gain_sum += diff;
        } else {
            loss_sum -= diff;
        }
    }
    let mut avg_gain = gain_sum / period as f64;
    let mut avg_loss = loss_sum / period as f64;
    result[period] = Some(if avg_loss == 0.0 {
        100.0
    } else {
        100.0 - 100.0 / (1.0 + avg_gain / avg_loss)
    });
    for i in (period + 1)..n {
        let diff = values[i] - values[i - 1];
        let gain = if diff > 0.0 { diff } else { 0.0 };
        let loss = if diff < 0.0 { -diff } else { 0.0 };
        avg_gain = (avg_gain * (period - 1) as f64 + gain) / period as f64;
        avg_loss = (avg_loss * (period - 1) as f64 + loss) / period as f64;
        result[i] = Some(if avg_loss == 0.0 {
            100.0
        } else {
            100.0 - 100.0 / (1.0 + avg_gain / avg_loss)
        });
    }
    result
}

/// Returns (upper, middle, lower).
pub fn calc_bb(
    values: &[f64],
    period: usize,
    mult: f64,
) -> (Vec<Option<f64>>, Vec<Option<f64>>, Vec<Option<f64>>) {
    let middle = calc_sma(values, period);
    let n = values.len();
    let mut upper = vec![None; n];
    let mut lower = vec![None; n];
    if period == 0 {
        return (upper, middle, lower);
    }
    for i in (period - 1)..n {
        if let Some(mid) = middle[i] {
            let start = i + 1 - period;
            let variance: f64 =
                values[start..=i].iter().map(|v| (v - mid).powi(2)).sum::<f64>() / period as f64;
            let stddev = variance.sqrt();
            upper[i] = Some(mid + mult * stddev);
            lower[i] = Some(mid - mult * stddev);
        }
    }
    (upper, middle, lower)
}

/// Returns (macd_line, signal_line, histogram). Uses standard (12, 26, 9) parameters.
pub fn calc_macd(
    values: &[f64],
) -> (Vec<Option<f64>>, Vec<Option<f64>>, Vec<Option<f64>>) {
    let ema12 = calc_ema(values, 12);
    let ema26 = calc_ema(values, 26);
    let n = values.len();
    let mut macd_line = vec![None; n];
    for i in 0..n {
        if let (Some(e12), Some(e26)) = (ema12[i], ema26[i]) {
            macd_line[i] = Some(e12 - e26);
        }
    }
    let first_macd = match macd_line.iter().position(|v| v.is_some()) {
        Some(p) => p,
        None => return (macd_line, vec![None; n], vec![None; n]),
    };
    let mut signal_line = vec![None; n];
    let mut histogram = vec![None; n];
    let signal_period = 9usize;
    if first_macd + signal_period > n {
        return (macd_line, signal_line, histogram);
    }
    let alpha = 2.0 / (signal_period as f64 + 1.0);
    let seed: f64 = macd_line[first_macd..first_macd + signal_period]
        .iter()
        .filter_map(|v| *v)
        .sum::<f64>()
        / signal_period as f64;
    let sig_start = first_macd + signal_period - 1;
    signal_line[sig_start] = Some(seed);
    histogram[sig_start] = macd_line[sig_start].map(|m| m - seed);
    let mut sig = seed;
    for i in (sig_start + 1)..n {
        if let Some(ml) = macd_line[i] {
            sig = alpha * ml + (1.0 - alpha) * sig;
            signal_line[i] = Some(sig);
            histogram[i] = Some(ml - sig);
        }
    }
    (macd_line, signal_line, histogram)
}
