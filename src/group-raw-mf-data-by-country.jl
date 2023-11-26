using DataFrames
using CSV
using Arrow
using Dates

include("shared/CommonConstants.jl")
include("shared/CommonFunctions.jl")
using .CommonFunctions
using .CommonConstants

const INPUT_DIR = joinpath(DIRS.fund, "raw")
const OUTPUT_DIR = joinpath(DIRS.fund, "domicile-grouped")

const EXPLICIT_TYPES = Dict(:FundId => String15, :SecId => String15)
const ID_COLS = [:name, :fundid, :secid]

function main()
    main_time_start = time()
    # Load info data first to build a map from fundid to domicile country group
    info = load_file_by_parts("info")

    info[!, :country_group] .= map_country_to_group.(info[!, :domicile])
    secid_to_group = Dict(zip(info[!, :secid], info[!, :country_group]))
    println("Regrouping files...")

    for folder in FIELD_FOLDERS
        regroup_data(folder, secid_to_group, info)
    end

    main_duration_s = round(time() - main_time_start, digits=2)
    main_duration_m = round(main_duration_s / 60, digits=2)
    println("Finished refining mutual fund data in $main_duration_s seconds " *
            "($main_duration_m minutes)")
end
    
function load_file_by_parts(folder)
    folderstring = joinpath(INPUT_DIR, folder)
    files = readdir(folderstring)
    data_parts = DataFrame[]

    for file in files
        filestring = joinpath(folderstring, file)
        
        if folder == "info"
            read_data = CSV.read(
                filestring, DataFrame, types=EXPLICIT_TYPES, stringtype=String,
                truestrings=["Yes"], falsestrings=["No"]
            )

            normalise_headings!(read_data, infodata=true)
        else
            read_data = CSV.read(
                filestring, DataFrame;
                types=EXPLICIT_TYPES, stringtype=String, groupmark=","
            )

            normalise_headings!(read_data)

            dropmissing!(read_data, [:fundid, :secid])
            _drop_incompleteids!(data_part)
            _drop_emptyrows!(data_part)
        end

        push!(data_parts, read_data)
    end

    data = vcat(data_parts...)
    folder != "info" && _drop_emptycols!(data)

    return data
end

function normalise_headings!(data_part; infodata=false)
    if infodata
        raw_names = names(data_part)
        re_underscored_chars = r"[\s\(\)\-]+(?!$)"
        re_erased_chars = r"[\s\(\)\-]+$"

        underscored_names = replace.(raw_names, re_underscored_chars => "_")
        tailclipped_names = replace.(underscored_names, re_erased_chars => "")
        normalised_names = replace.(tailclipped_names, r"&" => "and")

        rename!(data_part, lowercase.(normalised_names))
    else
        datestring = r"\d{4}-\d{2}"
        n_idcols = length(ID_COLS)
        n_datacols = ncol(data_part) - n_idcols
        first_datacol = n_idcols + 1
        
        start_date = match(datestring, names(data_part)[first_datacol]).match
        data_dates = [Date(start_date) + Month(i) for i in 0:(n_datacols-1)]
        date_names = Dates.format.(data_dates, "yyyy-mm") .|> Symbol
        column_names = [ID_COLS; date_names]
        rename!(data_part, column_names)
    end
end

function _drop_incompleteids!(df)
    incomplete_mask = any(_lostdata(df, [:fundid, :secid]), dims=2) |> vec
    incomplete_rows = (1:nrow(df))[incomplete_mask]
    deleteat!(df, incomplete_rows)
end

function _drop_emptycols!(df)
    empty_mask = all(_lostdata(df, Not(ID_COLS)), dims=1) |> vec
    empty_cols = names(df[!, Not(ID_COLS)])[empty_mask]
    select!(df, Not(empty_cols))
end

function _drop_emptyrows!(df)
    empty_mask = all(_lostdata(df, Not(ID_COLS)), dims=2) |> vec
    empty_rows = (1:nrow(df))[empty_mask]
    deleteat!(df, empty_rows)
end

function _lostdata(df, cols)
    islost = BitMatrix(undef, size(df[!, cols]))
    checkcols = eachcol(df[!, cols])
    for (i, col) in enumerate(checkcols)
        islost[:, i] .= ismissing.(col) .| isequal.(col, "")
    end
    return islost
end

function map_country_to_group(country::AbstractString)
    for (group, countries) in COUNTRY_GROUPS
        if country in countries
            return group
        end
    end

    return "other"
end
map_country_to_group(::Missing) = return "other"

function save_file_by_country_group(data, folder, group_map)
    for group in [keys(COUNTRY_GROUPS)..., "other"]
        # Split the dataframe into a subset of rows with the given country group
        data_split = data[map(secid -> get(group_map, secid, ""),
                          data[!, :secid]) .== group, :]
        
        output_filestring = makepath(OUTPUT_DIR, folder, "mf_$(folder)_$group.arrow")

        Arrow.write(output_filestring, data_split)
    end
end

function regroup_data(folder, secid_to_group, info)
    folder_time_start = time()
    if folder == "info"
        data = info[:, Not(:country_group)]
    else
        data = load_file_by_parts(folder)
    end

    save_file_by_country_group(data, folder, secid_to_group)
        
    folder_duration = round(time() - folder_time_start, digits=2)
    println("Processed folder $folder in $folder_duration seconds")
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end