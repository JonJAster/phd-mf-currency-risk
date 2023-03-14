using CSV
using DataFrames

const FIELD_FOLDERS = [
    "info", "local-monthly-gross-returns", "local-monthly-net-returns", "monthly-costs",
    "monthly-morningstar-category", "monthly-net-assets", "usd-monthly-gross-returns",
    "usd-monthly-net-returns"
]

const INPUT_FILESTRING_BASE = "./Data/Raw Data/Mutual Funds"
const OUTPUT_FILESTRING_BASE = "./Data/Refined Data/Mutual Funds/Cleaned Files"

function cleanfile!(data_part)
    normalise_headings!(data_part)
end

function normalise_headings!(data_part)
    raw_names = names(data_part)
    re_space_plus_newline_or_bracket = r"[^\S\r\n]+[\r\n\(]+"
    re_close_bracket = r"\)"
    re_remaining_whitespace = r"\s+"

    underscored_names = replace.(raw_names, re_space_plus_newline_or_bracket => "_")
    bracket_stripped_names = replace.(underscored_names, re_close_bracket => "")
    dashed_names = replace.(bracket_stripped_names, re_remaining_whitespace => "-")

    rename!(data_part, lowercase.(dashed_names))
end

function cleanup_folder(folder)
    input_folderstring = joinpath(INPUT_FILESTRING_BASE, folder)
    files = readdir(input_folderstring)
    for file in files
        input_filestring = joinpath(input_folderstring, file)
        
        data_part = CSV.read(input_filestring, DataFrame)

        cleanfile!(data_part)

        output_filestring = joinpath(OUTPUT_FILESTRING_BASE, folder, file)
        isdir(dirname(output_filestring)) || mkpath(dirname(output_filestring))
        CSV.write(output_filestring, data_part)
    end
end

function main()
    # The format of the csv files provided by Morningstar can cause slowdowns and errors
    # when processed in bulk.
    main_start_time = time()
    println("Cleaning csv files...")
    for folder in FIELD_FOLDERS
        folder_start_time = time()
        cleanup_folder(folder)
        
        folder_duration = round(time() - folder_start_time, digits=2)
        println("Finished cleaning $folder in $folder_duration seconds.")
    end

    main_duration = round(time() - main_start_time, digits=2)
    println("Finished cleaning all csv files in $main_duration seconds.")
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end