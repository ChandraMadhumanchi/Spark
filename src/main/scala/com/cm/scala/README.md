# Coding Test has 2 tasks 

Task 1 : implement the logic of a **left join** in scala with out using
  - Use language built-in functions for joining data
  - Use external libraries
  - Use SQL

Solution : 
  			I have implemented using plain scala
  			code file name is : JoinOperations.scala
  			code file found in below folder structure
  			 	 1stTask->src->main->scala->com->cm->scala> 
  			This "JoinOperations.scala" file accepts 2 command line arguments
  				 1. employee_names.csv and 2. employee_pay.csv file paths

 OutPut:
 

    id,first_name,last_name,salary,bonus
    59ea7840fc13ae1f6d000096,Benny,Ventom,$84217.20,
    59ea7840fc13ae1f6d000097,Cara,Motherwell,$54737.84,
    59ea7840fc13ae1f6d000098,Willem,Haresign,$92092.21,
    59ea7840fc13ae1f6d000099,Trish,Farlane,$86117.63,
    59ea7840fc13ae1f6d00009a,Camilla,Limpkin,$92821.29,
    59ea7840fc13ae1f6d00009b,Godwin,Caffrey,$60633.55,$1779.07
    59ea7840fc13ae1f6d00009c,Elvera,Custed,$94483.80,$4328.59
    59ea7840fc13ae1f6d00009d,Vinson,Farres,$93127.77,
    59ea7840fc13ae1f6d00009e,Kliment,Pitchford,$90581.06,
    59ea7840fc13ae1f6d00009f,Matty,Heater,$86947.54,
    59ea7840fc13ae1f6d0000a0,Juana,Begg,$80646.00,$2924.69
    59ea7840fc13ae1f6d0000a1,Alix,Layus,$50423.64,$1495.33
    59ea7840fc13ae1f6d0000a2,Tessa,Brandes,$60695.47,
    59ea7840fc13ae1f6d0000a3,Hedwig,Fishley,$85763.22,
    59ea7840fc13ae1f6d0000a4,Cordelia,Aubray,$60600.37,
   

2nd Task:

Assume the datasets provided above are very large (in millions ) 
and you need to perform the same left join in a distributed environment, 
where the input data files are stored in Hadoop. 
Please provide a pseudo code using java map reduce to solve the left outer join problem mentioned above. 
You do not have to write the exact java map reduce code. But need to provide map reduce algorithm details using pseudo code.

Solution :  found "2ndTask.pdf" file. Its contains pseudo code and step by step process.

