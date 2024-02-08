module CommonFunctions

using Revise
using CSV
using DataFrames
using Dates

include("CommonConstants.jl")
using .CommonConstants

export dirslist
export makepath
export drop_allmissing!

function dirslist()
    println("-- DIRS LIST --")
    println()
    println("----")
    category_names = Dict(
        :mf => "Mutual Funds",
        :fx => "Currencies",
        :eq => "Equities"
    )
    for category in keys(DIRS)
        println(category_names[category])
        for folder in keys(DIRS[category])
            println("DIRS.$category.$folder: $(DIRS[category][folder])")
        end
        println("----")
    end
end

function makepath(paths...)
    pathstring = joinpath(paths...)
    if match(FILE_SUFFIX, pathstring) |> isnothing
        dirstring = pathstring
    else
        dirstring = dirname(pathstring)
    end
    
    if !isdir(dirstring)
        mkpath(dirstring)
        println("Missing directory created: $dirstring")
    end

    return pathstring
end

function load_raw(filepath)
    data = CSV.read(filepath, DataFrame; stringtype=String, groupmark=',')
    _normalise_names!(data)
    drop_allmissing!(data, Not([:name, :fundid, :secid]); dims=:rows)
    return data
end

drop_allmissing!(df; dims=1) = drop_allmissing!(df, propertynames(df); dims=dims)
function drop_allmissing!(df, cols; dims=1)
    if dims ∉ [1, 2, :row, :rows, :col, :cols]
        error("dims must be :rows or :cols")
    end

    dimsmap = Dict(:row => 1, :rows => 1, :col => 2, :cols => 2)
    if dims ∉ [1, 2]
        dims = dimsmap[dims]
    end

    mask_matrix = .!(Matrix(df[!, cols]) .|> ismissing)
    if dims == 1
        one_vector = ones(size(mask_matrix,2))
        all_missing = mask_matrix * one_vector .== zero(size(mask_matrix,1))
        delete!(df, findall(all_missing))
    else
        one_vector = ones(size(mask_matrix,1))
        all_missing = mask_matrix' * one_vector .== zero(size(mask_matrix,2))
        select!(df, Not(cols[all_missing]))
    end
end

function _normalise_names!(df)
    n_id_cols = 3
    n_date_cols = ncol(df) - n_id_cols

    id_cols = names(df)[1:n_id_cols] .|> lowercase
    
    re_fieldname = r"^.+(?=\s?\r?\n\d{4}-\d{2})"
    re_date = r"(?<=\n)\d{4}-\d{2}"
    
    fieldname_match = match(re_fieldname, names(df)[n_id_cols + 1]).match
    fieldname = replace(fieldname_match, r"\r|\n| $" => "")
    
    start_date = match(re_date, names(df)[n_id_cols + 1]).match |> Dates.Date
    last_date = match(re_date, last(names(df))).match |> Dates.Date
    date_cols = [_offset_monthend(start_date, i) for i in 0:n_date_cols-1]

    last(date_cols)+Dates.Month(1) != last_date && @warn(
        "The calculated end date ($(last(date_cols))) is not the same as the last date " *
        "in the dataset for $fieldname ($last_date). This suggests that some date " *
        "columns may be missing or incorrectly sequenced."
    )

    name_map = zip(names(df), vcat(id_cols, date_cols))
    rename!(df, name_map)
    return
end

_offset_monthend(date, offset=1) = date + Dates.Month(offset)

end # module CommonFunctions
