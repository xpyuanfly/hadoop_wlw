package cn.hut.aop;

import java.util.Arrays;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.stereotype.Component;

@Aspect
@Component
public class LoggingAspect {
   
    @Before("execution(public int cn.hut.aop.ArithmeticCalculator.*(int, int))")
    public void beforeMethod(JoinPoint joinpoint){

        String methodName = joinpoint.getSignature().getName();
        Object [] args = joinpoint.getArgs();

        System.out.println("The method " + methodName + " begins with " + Arrays.asList(args));
    }
  
    @After("execution(* cn.hut.aop.*.*(..))")
    public void afterMethod(JoinPoint joinpoint){
        String methodName = joinpoint.getSignature().getName();
        System.out.println("The method " + methodName + " ends");
    }

}