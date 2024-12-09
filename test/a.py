files_active = [ os.path.join(dirpath, filename).replace(resource_directory,'') 
    for dirpath, dirnames, filenames in os.walk(resource_directory) for filename in filenames]
files_active = [item for item in files_active if search_string in item and 'cloudpsql' not in item]
