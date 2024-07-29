module CommonConstants

using DataStructures

export BETA_LAGS
export MIN_REGRESSION_OBS
export DEFAULT_DECAY
export TIMEWEIGHT_LAGS
export FLOW_CONTROL_LAGS
export DIRS
export EQUITY_LMS_FACTORS
export MODELS

const BETA_LAGS = 60
const MIN_REGRESSION_OBS = 36
const DEFAULT_DECAY = 0.186
const TIMEWEIGHT_LAGS = 18
const FLOW_CONTROL_LAGS = 19

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
        weighted = "data/combined/weighted",
        flow_betas = "data/combined/flow-betas"
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

const MODELS = OrderedDict(
    :ff_usa_ffc4 => ("ff_usa", [:mkt, :smb, :hml, :wml]),
    :ff_usa_ffc6 => ("ff_usa", [:mkt, :smb, :hml, :rmw, :cma, :wml]),
    :ff_usa_ffc6_ver => (
        "ff_usa", [:mkt, :smb, :hml, :rmw, :cma, :wml, :dollar, :carry]
    ),
    :ff_dev_ffc4 => ("ff_dev", [:mkt, :smb, :hml, :wml]),
    :ff_dev_ffc6 => ("ff_dev", [:mkt, :smb, :hml, :rmw, :cma, :wml]),
    :ff_dev_ffc6_ver => (
        "ff_dev", [:mkt, :smb, :hml, :rmw, :cma, :wml, :dollar, :carry]
    )
)

end # module CommonConstants