package com.aj;

import java.io.File;
import java.net.URISyntaxException;
import java.util.Arrays;

public class Main {

    public static void main(String[] args) throws URISyntaxException {
        String jarLocation = Main.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
        jarLocation = jarLocation.substring(0, jarLocation.lastIndexOf(File.separator) + 1);

        File rootDirectory = new File(jarLocation);

        File[] subDirectories = rootDirectory.listFiles();
        Arrays.stream(subDirectories).filter(File::isDirectory).map(String::valueOf).forEach(SQLImporter::new);

        /*System.out.println("jarLocation : " + jarLocation);
        System.out.println("subDirectories : " + subDirectories);
        System.out.println("subDirectories : " + subDirectories.length);

        Arrays.stream(subDirectories).forEach(System.out::println);*/
    }


}
