package cn.hut.aop;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

/**
 * Spring上下文工具类，用以让普通类获取Spring容器中的Bean
 */
@Component
public class SpringUtil implements ApplicationContextAware {

    private static ApplicationContext applicationContext = null;

    // 获取applicationContext
    public static ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        if (SpringUtil.applicationContext == null) {
            SpringUtil.applicationContext = applicationContext;
        }
    }

    public static void showAllbeans() {        
        String[] str = applicationContext.getBeanDefinitionNames();
        for (String string : str) {
            System.out.println("..." + string);
        }
    }

    // 通过name获取 Bean
    public static Object getBean(String name) {
        return getApplicationContext().getBean(name);
    }
}