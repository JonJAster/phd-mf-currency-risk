const FIELD_FOLDERS = [
    "info", "local-monthly-gross-returns", "local-monthly-net-returns", "monthly-costs",
    "monthly-morningstar-category", "monthly-net-assets", "usd-monthly-gross-returns",
    "usd-monthly-net-returns"
]

const INPUT_FILESTRING_BASE = "./Data/Raw Data/Mutual Funds"
const OUTPUT_FILESTRING_BASE = "./Data/Refined Data/Mutual Funds/Cleaned Files"
const PERMISSION_DENIED = 13

macro TryIO(expr, timeout_seconds=120, wait_seconds=10)
    quote
        start_time = time()
        duration = 0
        success = false
        while duration < $timeout_seconds
            try
                result = $(esc(expr))
                success = true
                break
            catch e
                ioerror = e isa Base.IOError
                permission_denied = e isa SystemError && e.errnum == PERMISSION_DENIED

                if ioerror || permission_denied
                    sleep($wait_seconds)
                    duration = round(time() - start_time, digits=2)
                else
                    rethrow(e)
                end
            end
        end
        success || error("Timed out after $timeout_seconds seconds.")
    end
end
SystemError

function cleanup_folder(folder)
    folderstring = joinpath(OUTPUT_FILESTRING_BASE, folder)

    files = readdir(folderstring)
    for file in files
        filestring = joinpath(folderstring, file)
        cleanfile(filestring)
    end
end

function cleanfile(data_part)
    open(data_part, "r+") do file
        @TryIO content = read(file, String)

        content = remove_empty_quotes(content)
        content = remove_linefeed_chars_in_values(content)
        #content = remove_delimeters_in_values(content)

        seek(file, 0)
        @TryIO write(file, content)
        @TryIO truncate(file, position(file))
        close(file)
    end
end

remove_empty_quotes(string) = replace(string, r"\"\"" => "")
remove_linefeed_chars_in_values(string) = replace(string, r"(?!<\r)\n" => "")
#remove_delimeters_in_values(string) = replace(string, r"," => "")

function main()
    # The format of the csv files provided by Morningstar can cause slowdowns and errors
    # when processed in bulk.
    main_start_time = time()

    copy_start_time = time()
    println("Copying csv files...")
    @TryIO cp(INPUT_FILESTRING_BASE, OUTPUT_FILESTRING_BASE, force=true)
    copy_duration = round(time() - copy_start_time, digits=2)
    println("Finished copying csv files in $copy_duration seconds.")

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