using DataFrames
using CSV
using Arrow

includet("../../shared/CommonConstants.jl")
includet("../../shared/CommonFunctions.jl")

using .CommonConstants
using .CommonFunctions

const N_ID_COLS = 3
const DATEFORMAT = r"^\d{4}-\d{2}"

function init_mf_data()
    mf_data_collection = _read_mf_data()
    mf_data = _merge_mf_data(mf_data_collection)
end

function _read_mf_data()
    folder = DIRS.mf.raw
    files = readdir(folder)
    data = Dict()
    for file in files
        data[file] = CSV.read(joinpath(folder, file), DataFrame)
        _normalise_names!(data[file])
    end
    return data
end

function _merge_mf_data(data_collection)
    data = DataFrame()
    for (datafield, df) in data_collection
        drop_allmissing!(df, dims=:rows)

        melted_df = stack(
            df, Not(:fundid, :secid, :name), [:fundid, :secid],
            variable_name=:date, value_name=datafield
        )
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    init_mf_data()
end