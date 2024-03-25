module CommonConstants

export BETA_LAGS
export MIN_REGRESSION_OBS
export DEFAULT_DECAY
export TIMEWEIGHT_LAGS
export AGE_FILTER
export FLOW_CONTROL_LAGS
export DIRS
export EQUITY_LMS_FACTORS
export MODELS

const BETA_LAGS = 60
const MIN_REGRESSION_OBS = 36
const DEFAULT_DECAY = 0.186
const TIMEWEIGHT_LAGS = 18
const AGE_FILTER = 36
const FLOW_CONTROL_LAGS = 19

const HACKDIR = "data/test/old-comparison-data/new-format/magnitude-adjusted"
const TEST_STATUS = 2
const DIRS = (
    mf = (
        raw = "data/mutual-funds/raw",
        init = "data/mutual-funds/init",
        refined = [HACKDIR, "data/mutual-funds/refined"][TEST_STATUS]
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
        factors = [HACKDIR, "data/combined/factors"][TEST_STATUS],
        return_betas = [HACKDIR, "data/combined/return-betas"][TEST_STATUS],
        decomposed = [joinpath(HACKDIR, "decomposed"), "data/combined/decomposed"][TEST_STATUS],
        weighted = [joinpath(HACKDIR, "weighted"), "data/combined/weighted"][TEST_STATUS],
        flow_betas = [joinpath(HACKDIR, "flow-betas"), "data/combined/flow-betas"][TEST_STATUS]
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
    "dev_ff3_ver" => ("DEV", [:mkt, :smb, :hml, :dollar, :carry])
)

end # module CommonConstants