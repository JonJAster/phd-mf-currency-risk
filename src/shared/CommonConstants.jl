module CommonConstants

export BETA_LAGS
export MIN_REGRESSION_OBS
export DEFAULT_DECAY
export TIMEWEIGHT_LAGS
export DIRS
export EQUITY_LMS_FACTORS
export MODELS

const BETA_LAGS = 60
const MIN_REGRESSION_OBS = 36
const DEFAULT_DECAY = 0.186
const TIMEWEIGHT_LAGS = 18

const DIRS = (
    mf = (
        raw = "data/mutual-funds/raw",
        init = "data/mutual-funds/init",
        refined = "data/mutual-funds/refined"
    ),
    fx = (
        raw = "data/currencies/raw",
        refined = "data/currencies/refined",
        factors = "data/currencies/factors"
    ),
    eq = (
        raw = "data/equity-factors/raw",
        refined = "data/equity-factors/refined",
        factors = "data/equity-factors/factors"
    ),
    combo = (
        factors = "data/combined/factors",
        return_betas = "data/combined/return-betas",
        decomposed = "data/combined/decomposed",
        weighted = "data/combined/weighted"
    ),
    map = (
        raw = "data/maps/raw",
        refined = "data/maps/refined"
    ),
    test = "data/test"
)

const EQUITY_LMS_FACTORS = Dict(
    "market_equity" => "smb",
    "be_me" => "hml",
    "ret_12_1" => "wml",
    "ope_be" => "rmw",
    "at_gr1" => "cma"
)

const MODELS = Dict(
    "usa_capm" => ("USA", [:mkt]),
    "usa_capm_lrv" => ("USA", [:mkt, :rx, :hml_fx]),
    "usa_capm_ver" => ("USA", [:mkt, :dollar, :carry]),
    "usa_ff3" => ("USA", [:mkt, :smb, :hml]),
    "usa_ff3_lrv" => ("USA", [:mkt, :smb, :hml, :rx, :hml_fx]),
    "usa_ff3_ver" => ("USA", [:mkt, :smb, :hml, :dollar, :carry]),
    "wld_capm" => ("WLD", [:mkt]),
    "wld_capm_lrv" => ("WLD", [:mkt, :rx, :hml_fx]),
    "wld_capm_ver" => ("WLD", [:mkt, :dollar, :carry]),
    "wld_ff3" => ("WLD", [:mkt, :smb, :hml]),
    "wld_ff3_lrv" => ("WLD", [:mkt, :smb, :hml, :rx, :hml_fx]),
    "wld_ff3_ver" => ("WLD", [:mkt, :smb, :hml, :dollar, :carry]),
    "dev_capm" => ("DEV", [:mkt]),
    "dev_capm_lrv" => ("DEV", [:mkt, :rx, :hml_fx]),
    "dev_capm_ver" => ("DEV", [:mkt, :dollar, :carry]),
    "dev_ff3" => ("DEV", [:mkt, :smb, :hml]),
    "dev_ff3_lrv" => ("DEV", [:mkt, :smb, :hml, :rx, :hml_fx]),
    "dev_ff3_ver" => ("DEV", [:mkt, :smb, :hml, :dollar, :carry]),
    "emg_capm" => ("EMG", [:mkt]),
    "emg_capm_lrv" => ("EMG", [:mkt, :rx, :hml_fx]),
    "emg_capm_ver" => ("EMG", [:mkt, :dollar, :carry]),
    "emg_ff3" => ("EMG", [:mkt, :smb, :hml]),
    "emg_ff3_lrv" => ("EMG", [:mkt, :smb, :hml, :rx, :hml_fx]),
    "emg_ff3_ver" => ("EMG", [:mkt, :smb, :hml, :dollar, :carry])
)

end # module CommonConstants