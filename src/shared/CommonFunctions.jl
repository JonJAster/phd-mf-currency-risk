module CommonFunctions

using DataFrames
using Arrow
using CSV
using Dates
using StatsBase

include("CommonConstants.jl")
using .CommonConstants

export dirslist
export makepath
export qhead
export qscan
export qlookup
export printtime
export init_raw
export _drop_allmissing!

const FILE_SUFFIX = r"\.[a-zA-Z0-9]+$"

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

function qhead(filename)
    data = Arrow.Table(filename)
    output = propertynames(data)
    return output
end

function qscan(filename)
    data = Arrow.Table(filename)
    println("------")
    for col in propertynames(data)
        println(col)
        println()
        describe(data[col])
        println("------")
    end
    return
end

function qlookup(id; data=false)
    data ? filestring = "mf-data.arrow" : filestring = "mf-info.arrow"
    pathstring = joinpath(DIRS.mf.init, filestring)

    data = Arrow.Table(pathstring) |> DataFrame

    output = data[data.fundid .== id, :]
    nrow(output) == 0 && (output = data[data.secid .== id, :])

    return output
end

function printtime(
        task, start_time;
        process_subtask="", process_start_time=0, minutes=false
        )
    if isempty(process_subtask) ⊻ iszero(process_start_time)
        error("If any process parameters are supplied, all must be supplied")
    end
    timed_process = !isempty(process_subtask)

    duration_s = round(time() - start_time, digits=2)
    duration_m = round(duration_s / 60, digits=2)
    
    if !timed_process
        printout = "Finished $task in $duration_s seconds"
        minutes && (printout *= " ($duration_m minutes)")
    else
        process_duration_s = round(time() - process_start_time, digits=2)
        process_duration_m = round(process_duration_s / 60, digits=2)

        printout = "Finished $task for $process_subtask in $process_duration_s seconds"
        minutes && (printout *= " ($process_duration_m minutes)")
        printout *= ", total running time $duration_s seconds ($duration_m minutes)"
    end

    println(printout)
    return nothing
end

function init_raw(filepath; info=false)
    if info
        data = CSV.read(filepath, DataFrame; truestrings=["Yes"], falsestrings=["No"])
        _normalise_names!(data; info=true)
        _null_empty_strings!(data)
    else
        data = CSV.read(filepath, DataFrame; stringtype=String, groupmark=',')
        _normalise_names!(data)
        _drop_allmissing!(data, dims=:cols)
        _drop_allmissing!(data, Not([:name, :fundid, :secid]); dims=:rows)
    end
    return data
end

_drop_allmissing!(df; dims=1) = _drop_allmissing!(df, propertynames(df); dims=dims)
function _drop_allmissing!(df, cols; dims=1)
    if dims ∉ [1, 2, :row, :rows, :col, :cols]
        error("dims must be :rows or :cols")
    end

    dimsmap = Dict(:row => 1, :rows => 1, :col => 2, :cols => 2)
    if dims ∉ [1, 2]
        dims = dimsmap[dims]
    end

    testdf = DataFrame(
        name = ["A", "B", "C", "D", "E"],
        fundid = [1, 2, 3, 4, 5],
        secid = [1, 2, 3, 4, 5],
        date = ["2020-01", "2020-02", "2020-03", "2020-04", "2020-05"],
        value = [missing, missing, missing, missing, missing]
    )
    df = copy(testdf)
    cols = names(df)
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

function _normalise_names!(df; info=false)
    if info
        re_invalidchars_nonend = r"[^a-zA-Z0-9]+(?!$)"
        re_invalidchars_end = r"[^a-zA-Z0-9]+$"

        function namemap(x)
            replace(x, re_invalidchars_nonend => "_") |> x ->
            replace(x, re_invalidchars_end => "") |>
            lowercase
        end

        new_names = names(df) .|> namemap
        rename!(df, new_names)
    else
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

        last(date_cols) != last_date && @warn(
            "The calculated end date ($(last(date_cols))) is not the same as the last date " *
            "in the dataset for $fieldname ($last_date). This suggests that some date " *
            "columns may be missing or incorrectly sequenced."
        )

        rename!(df, Symbol.([id_cols; date_cols]))
    end
    return
end

function _null_empty_strings!(df)
    for col in propertynames(df)
        if count(coalesce.(df[!, col] .== "", false)) > 0
            df[!, col] = replace(df[!, col], "" => missing)
        end
    end
    return
end

_offset_monthend(date, offset=1) = date + Dates.Month(offset)

end # module CommonFunctions
