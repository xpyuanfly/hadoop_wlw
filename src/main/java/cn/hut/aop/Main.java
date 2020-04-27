package cn.hut.aop;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.stereotype.Component;

@Component
@EnableAspectJAutoProxy
@ComponentScan("cn.hut.aop")
@SpringBootApplication
public class Main {

    @Autowired // 依赖的注入 ==> 注入的对象来自于容器(ioc容器)
    private static ArithmeticCalculator arithmeticCalculator;

    public static void main(final String[] args) {
        SpringApplication.run(Main.class, args);        
        SpringUtil.showAllbeans();
        arithmeticCalculator= (ArithmeticCalculator) SpringUtil.getBean("ArithmeticCalculator"); 
        testAop();
    }
    
    public static void testAop(){
        
        // ArithmeticCalculatorWithlogsImpl 的加法
        System.out.println("--------------ArithmeticCalculatorImpl add-----------------");
        final int i = 5;
        final int j = 3;
        final int result = arithmeticCalculator.add(i, j);
        System.out.println("result:"+result);
        System.out.println("--------------ArithmeticCalculatorImpl end-----------------");

        // System.out.println("--------------ArithmeticCalculatorProxy
        // add-----------------");
        // ArithmeticCalculatorProxy builder = new
        // ArithmeticCalculatorProxy(arithmeticCalculator);
        // ArithmeticCalculator proxy = builder.getProxy();
        // proxy.add(i, j);
    }
}