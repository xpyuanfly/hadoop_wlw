package cn.hut.aop;
import org.springframework.stereotype.Service;

// @Service("ArithmeticCalculatorWithlogsImpl")
public class ArithmeticCalculatorWithlogsImpl implements ArithmeticCalculator{
    
    public ArithmeticCalculatorWithlogsImpl(){
        super();
    }

    @Override
    public int add(int i, int j) {
        System.out.println("number1:"+i+"number2:"+j);
        int result = i + j;
        System.out.println("result:"+result);
        return result;
    }

    @Override
    public int sub(int i, int j) {
        System.out.println("number1:"+i+"number2:"+j);
        int result = i - j;
        System.out.println("result:"+result);
        return result;
    }

    @Override
    public int mul(int i, int j) {
        System.out.println("number1:"+i+"number2:"+j);
        int result = i * j;
        System.out.println("result:"+result);
        return result;
    }

    @Override
    public int div(int i, int j) {
        System.out.println("number1:"+i+"number2:"+j);
        int result = i /j; 
        System.out.println("result:"+result);
        return result;
    }
    

}