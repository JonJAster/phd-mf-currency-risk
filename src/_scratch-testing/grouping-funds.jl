includet("../pipeline/processing-funds/group-raw-mf-data-by-country.jl")

@time d = _loaddata(qpath(PATHS.rawfunds, FUND_FIELDS.na))