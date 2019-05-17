import os
import os.path

def replace_fn(parent_directory):
    for path, folder_list, file_list in os.walk(parent_directory):
        for f in file_list:
            f_clean = f.replace(",", ".").strip()
            f_clean = f_clean.replace(' ', '_')
            os.rename(os.path.join(path, f), os.path.join(path, f_clean))
            print("Old fname: ", f, " New fname: ", f_clean)
            print('\n')
        for i in range(len(folder_list)):
            folder_clean = folder_list[i].replace(",", ".").strip()
            folder_clean = folder_clean.replace(" ", "_")
            os.rename(os.path.join(path, folder_list[i]), os.path.join(path, folder_clean))
            print("Old folder_name: ", folder_list[i], " New folder_name: ", folder_clean)
            folder_list[i] = folder_clean
    return 'Completed process'


def main():
    p_dir = '/mnt/data/projects/el_salvador_mined_education/datos_enviar_UChicago_20152018/8._Listado_de_Centros_educativos_por_indice_de_Priorizacion_de_Centros_educativos/2016/4_Bajo'
    #p_dir = "parent directory"
    #print(os.listdir(p_dir))
    replace_fn(p_dir)


if __name__ == "__main__":
    main()
