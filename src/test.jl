using
    DataFrames,
    Arrow,
    Dates,
    Statistics

using ShiftedArrays: lag

include("shared/CommonConstants.jl")
include("shared/CommonFunctions.jl")
include("shared/DataInit.jl")
using
    .CommonConstants,
    .CommonFunctions,
    .DataInit

const OPTION_FOLDER = option_foldername(; DEFAULT_OPTIONS...)
const TEST_FOLDERNAME = (
    joinpath(DIRS.fund, "post-processing", OPTION_FOLDER, "decompositions")
)

modelfiles = readdir(TEST_FOLDERNAME)
testfile = first(modelfiles)
testpath = joinpath(TEST_FOLDERNAME, testfile)

data = Arrow.Table(testpath) |> DataFrame

function rolling_std(data, col, window)
    data[!, :std] = Vector{Union{Missing, Float64}}(missing, size(data, 1))

    for i in 1:size(data, 1)
        i < window && continue
        window_start = i - window + 1
        window_end = i

        data[window_start, :fundid] != data[window_end, :fundid] && continue

        start_date = Dates.lastdayofmonth((data[window_end, :date] - Month(window-1)))
        data[window_start, :date] != start_date && continue
        data[i, :std] = std(data[window_start:window_end, col])
    end
end