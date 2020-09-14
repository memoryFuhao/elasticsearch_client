package com.elasticsearch.vo;

import com.elasticsearch.common.annotation.Index;
import com.elasticsearch.common.annotation.Propertie;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import lombok.Data;

/**
 * Created by memory_fu on 2020/8/31.
 */
@Data
@Index(IndexName = "person_index")
public class Person extends Parent {
    
    @Propertie(PropertieName = "hobby")
    String hobby;
    
    public static void main(String[] args) {
        
        Person person = new Person();
        Class<? extends Person> aClass = person.getClass();
        Class<?> superclass = aClass.getSuperclass();
        Class<?> superclass1 = superclass.getSuperclass();
        
        for (Class i = aClass; i != Object.class; i = i.getSuperclass()) {
            
            Field[] declaredFields = i.getDeclaredFields();
            for (Field field : declaredFields) {
                String fieldName = field.getName();
                Annotation[] annotations = field.getAnnotations();
                for (Annotation an : annotations) {
                    if (an instanceof Propertie) {
                        String propertieName = ((Propertie) an).PropertieName();
                        System.out.println(propertieName);
                    }
                }
            }
            
        }
        
        System.out.println(aClass);
        System.out.println(superclass);
        System.out.println(superclass1);
    }
}
