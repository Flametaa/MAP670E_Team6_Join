import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileManager {
    String path;

    File file ;
    BufferedWriter resultBuffer;



    public FileManager(String path){
        this.path = path;

        file = new File(path);
    }


    public void createDirectory(){
        Path pa = Paths.get(this.path);

        try {
            Files.createDirectories(pa);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void createFile(){
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }



        FileWriter fw = null;
        try {
            fw = new FileWriter(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.resultBuffer = new BufferedWriter(fw);


    }

    public void writeOnFile(String text){
        try {
            resultBuffer.write(text);


        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    public void flushBuffer(){
        try {
            resultBuffer.flush();


        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    public void closeFile(){
        try {
            resultBuffer.flush();
            resultBuffer.close();
        } catch (IOException e) {


        }
    }

    public void deleteFile(){
        Path pa = Paths.get(this.path);

        try {
            Files.delete(pa);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
