package ict.ocrabase.main.java.query.admaster.select;

import java.util.*;  
class Test  
{  
    private static class ValueComparator implements Comparator<Map.Entry<String,Integer>>  
    {  
        public int compare(Map.Entry<String,Integer> m,Map.Entry<String,Integer> n)  
        {  
            return n.getValue()-m.getValue();  
        }  
    }  
    public static void main(String[] args)   
    {  
        Map<String,Integer> map=new HashMap<>();  
        map.put("a",1);  
        map.put("c",3);  
        map.put("b",5);  
        map.put("f",7);  
        map.put("e",6);  
        map.put("d",8);  
        List<Map.Entry<String,Integer>> list=new ArrayList<>();  
        list.addAll(map.entrySet());  
        Test.ValueComparator vc=new ValueComparator();  
        Collections.sort(list,vc);  
        for(Iterator<Map.Entry<String,Integer>> it=list.iterator();it.hasNext();)  
        {  
            System.out.println(it.next());  
        }  
    }  
}  
