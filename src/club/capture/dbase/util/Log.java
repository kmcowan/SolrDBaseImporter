/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package club.capture.dbase.util;

/**
 *
 * @author kevin
 */
public class Log {
    
    public static void log(String message){
        System.out.println(message);
    }
    
    public static void log(Class cls, String message){
        System.out.println("["+ cls.getSimpleName()+"] "+ message);
    }
}
