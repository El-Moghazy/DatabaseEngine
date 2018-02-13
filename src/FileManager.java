import java.io.File;

public class FileManager {

    static boolean createFolder(String name,String path) {

        try {

            File file = new File(path + name);
            if (!file.exists()) {
                if (file.mkdir()) {
                    System.out.println("Directory is created!");

                } else {
                    System.out.println("Failed to create directory!");
                }
            }

        } catch (Exception e) {

        }
        return false;
    }

    public static void main(String[] args) {

        createFolder("db", "");

    }

}
