module CommonConstants

export DIRS

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
        raw = "data/equities/raw",
        refined = "data/currencies/refined",
        factors = "data/currencies/factors"
    ),
    map = (
        raw = "data/maps/raw",
        refined = "data/maps/refined"
    ),
    test = "data/test"
)

end # module CommonConstants