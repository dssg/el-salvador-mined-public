import os
import os.path

def list_files(startpath):
    results_list = []
    for root, dirs, files in os.walk(startpath):
        level = root.replace(startpath, '').count(os.sep)
        indent = ' ' * 4 * (level)
        results_list.append('{}{}/'.format(indent, os.path.basename(root)))
        subindent = ' ' * 4 * (level + 1)
        for f in files:
            results_list.append('{}{}'.format(subindent,f))
    return results_list


def main():
    project_dir = '/mnt/data/projects/el_salvador_mined_education/'
    data_dir = 'datos_enviar_UChicago_20152018'
    output_file = 'directory_structure_20180618.txt'

    files_list = list_files(os.path.join(project_dir, data_dir))
    
    f = open(os.path.join(project_dir, 'dhany', output_file), 'w')
    for line in files_list:
        f.write('\n')
        print(line)
        f.write(line)
    
    f.close()


if __name__ == "__main__":
    main()
