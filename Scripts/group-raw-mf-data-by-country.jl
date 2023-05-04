using DataFrames
using CSV
using Dates

include("CommonConstants.jl")
using .CommonConstants

const INPUT_FILESTRING_BASE = "./data/raw/mutual-funds"
const OUTPUT_FILESTRING_BASE = "./data/prepared/mutual-funds"
const DATESTRING = r"\d{4}-\d{2}"

const EXPLICIT_TYPES = Dict(:FundId => String15, :SecId => String15)
const DATA_COLS = Not([:name, :fundid, :secid])
const NONDATA_COL_OFFSET = 3

filestring = "./data/raw/mutual-funds/monthly-net-assets/mf_monthly-net-assets_part-1.csv"

ismissing_or_blank(x) = ismissing(x) || x == ""

function load_file_by_parts(folder)
    folderstring = joinpath(INPUT_FILESTRING_BASE, folder)
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
                filestring, DataFrame, types=EXPLICIT_TYPES, stringtype=String,
            )

            normalise_headings!(read_data)

            dropmissing!(read_data, [:fundid, :secid])
            read_data = (fix_thousands_commas ∘ drop_blank_ids ∘ drop_empty_rows)(read_data)
        end

        push!(data_parts, read_data)
    end

    data = vcat(data_parts...) |> drop_empty_cols
    return data
end

function drop_blank_ids(df)
    return df[.!ismissing_or_blank.(df.fundid) .&& .!ismissing_or_blank.(df.secid), :]
end

function normalise_headings!(data_part; infodata=false)
    if infodata
        raw_names = names(data_part)
        re_whitespace_plus_bracket = r"\s+\("
        re_close_bracket = r"\)"
        re_remaining_whitespace = r"\s+"

        underscored_names = replace.(raw_names, re_whitespace_plus_bracket => "_")
        bracket_stripped_names = replace.(underscored_names, re_close_bracket => "")
        dashed_names = replace.(bracket_stripped_names, re_remaining_whitespace => "-")

        rename!(data_part, lowercase.(dashed_names))
    else
        start_date = match(DATESTRING, names(data_part)[4]).match
            number_of_other_dates = size(data_part, 2) - 4
            data_dates = [Date(start_date) + Month(i) for i in 0:number_of_other_dates]
            
            column_names = Symbol.(
                [lowercase.(names(data_part)[1:3]); Dates.format.(data_dates, "yyyy-mm")]
            )
        rename!(data_part, column_names)
    end
end

function fix_thousands_commas(df)
    col_types = map(col -> eltype(col), eachcol(df[!, DATA_COLS]))
    isstring(T) = T <: Union{Missing, AbstractString} && T != Missing
    isnumber(T) = T <: Union{Missing, Number} && T != Missing
    data_contains_strings = any(isstring, col_types)
    data_contains_numbers= any(isnumber, col_types)

    mistyped_cols = Int[]
    if data_contains_strings && data_contains_numbers
        mistyped_cols = findall(isstring, col_types) .+ NONDATA_COL_OFFSET
    elseif data_contains_strings
        data_query_idx = findfirst(!ismissing_or_blank, df[!, end])
        data_query = df[data_query_idx, end]
        valid_number = !occursin(r"[^\d.,]", data_query)
        valid_number && (mistyped_cols = (1:length(col_types)) .+ NONDATA_COL_OFFSET)
    end

    isempty(mistyped_cols) && return df
    return strip_thousands_commas(df, mistyped_cols)
end

function strip_thousands_commas(df, mistyped_cols)
    strip_commas(x) = ismissing(x) ? missing : replace(x, ","=>"")

    for col in mistyped_cols
        df[!, col] = strip_commas.(df[!, col])
    end

    return df
end

function drop_empty_rows(df)
    where_data_exists = .!ismissing_or_blank.(df[!, DATA_COLS])
    any_data = reduce(.|, eachcol(where_data_exists))
    return df[any_data, :]
end

function drop_empty_cols(df)
    where_data_exists = .!ismissing_or_blank.(df)
    any_data = any.(eachcol(where_data_exists))
    return df[:, any_data]
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

        filestring = joinpath(OUTPUT_FILESTRING_BASE, folder, "mf_$(folder)_$group.csv")

        if !isdir(dirname(filestring))
            mkpath(dirname(filestring))
        end

        CSV.write(filestring, data_split)
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

    main_duration = round(time() - main_time_start, digits=2)
    println("Finished refining mutual fund data in $main_duration seconds")
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end

# Set filestring to net assets "other" group from the prepared folder
filestring = joinpath(OUTPUT_FILESTRING_BASE, "monthly-net-assets", "mf_monthly-net-assets_other.csv")
data = CSV.read(filestring, DataFrame)