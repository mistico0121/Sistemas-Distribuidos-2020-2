package main

import (
    "bufio"   // Leer STDIN
    //"flag"    // Leer flags
    "fmt"     // Print
    "log"     // Escribir Logs
    "os"      // Leer sistema de archivos
    //"runtime" // Imprimir # de GoRouitnes
    //"strings" // Usar Replace()
    "sync"
    "encoding/csv"     // Leer archivos csv
    "strings"

    "time"

    //"strconv" // Convertir de string a int
)

// filename = Covid-19_std.csv

type M map[string]string

//CON ESTO HAREMOS UN DICCIONARIO ESPECIAL TAL QUE LA LLAVE ES UNA LISTA DE STRINGS
type Key struct {
    f []string
}

type structData struct {

    Region string
    Codigo_Region string
    Comuna string
    Codigo_Comuna string
    Poblacion string
    Fecha string
    Casos string
}

func checkError(message string, err error) {
    if err != nil {
        log.Fatal(message, err)
    }
}


/*
func projectionFunction(numeroo int){
    fmt.Println(numeroo)

}
*/

/*
func groupAggregateFunction(col_name0, col_name1, functionSelect string){
    fmt.Println(col_name0)
    fmt.Println(col_name1)
    fmt.Println(functionSelect)
}
*/

//Entra una linea del csv en forma de lista de strings
//Sale una lista de map
func MapSelect(line []string)[]M{

    var mapSlice []M

    //selectedValue, _ := strconv.Atoi(line[3])

    m1 := M{
        "Region": line[0],
        "Codigo region": line[1],
        "Comuna": line[2],
        "Codigo comuna": line[3],
        "Poblacion": line[4],
        "Fecha": line[5],
        "Casos confirmados": line[6],
    }

    mapSlice = append(mapSlice, m1)

    return mapSlice

}

func mapToCSV(mapList []M, operation string){

    stringfilename := fmt.Sprintf("%s%s",operation,".csv")

    
    file, err := os.Create(stringfilename)
    checkError("Cannot create file", err)
    defer file.Close()

    writer := csv.NewWriter(file)
    defer writer.Flush()
    

    fmt.Println(stringfilename)

    for _, list:= range mapList{
        
        var s []string

        //index, M
        for _, EME := range list{
            
            s = append(s, EME)
            
        }

        err := writer.Write(s)
        checkError("Cannot write to file", err)
    }
}

func MapProjection(keysToUse []string, line []string)[]M{

    var mapSlice []M

    //Auxiliar
    m1 := M{
        "Region": line[0],
        "Codigo region": line[1],
        "Comuna": line[2],
        "Codigo comuna": line[3],
        "Poblacion": line[4],
        "Fecha": line[5],
        "Casos confirmados": line[6],
    }

    req := M{}

    for _, neededKey := range keysToUse{
        req[neededKey] = m1[neededKey]
    }

    mapSlice = append(mapSlice, req)

    return mapSlice

}


//Entra una linea del csv en forma de lista de strings
//Sale una lista de structData
/*
func MapP(line []string, atrIndex int)[]structData{
    list := []structData{}

    selectedValue, _ := line[atrIndex]

    list.append(list, structData{

    })

}
*/



func ReduceProjection(mapList chan[]M, sendFinalValue chan[]M){

    var final []M

    for list:= range mapList{
            for _, value := range list{

                final = append(final, value)
                
            }
        }
    sendFinalValue <- final
}

func timeEvaluator(timestring string) date{
    t, err := time.Parse("2006-01-02", "2011-01-19")

    return t
}


func ReduceSelect(colname string, operation string, compare_to string, mapList chan[]M, sendFinalValue chan[]M){
    var final []M

    //Decidí que la iteracion esté dentro del switch para que así solo evalúe el switch 1 vez en vez de cada vez por elemento
    switch operation{
    case ">":
        for list:= range mapList{
            for _, value := range list{
                if (value[colname] > compare_to){
                    final = append(final, value)
                }
            }
        }
    
    case "<":
        for list:= range mapList{
            for _, value := range list{
                if value[colname] < compare_to{
                    final = append(final,value)
                }
            }
        }
    
    case ">=":
        for list:= range mapList{
            for _, value := range list{
                if value[colname] >= compare_to{
                    final = append(final, value)
                }
            }
        }
    
    case "<=":
        for list:= range mapList{
            for _, value := range list{
                if value[colname] <= compare_to{
                    final = append(final, value)
                }
            }
        }
    
    case "==":
        for list:= range mapList{
            for _, value := range list{
                if value[colname] == compare_to{
                    final = append(final, value)
                }
            }
        }
    
    case "!=":
        for list:= range mapList{
            for _, value := range list{
                if value[colname] != compare_to{
                    final = append(final, value)
                }
            }
        }
    }

    fmt.Println(final)


    sendFinalValue <- final
}

func main() {

    csvFile, err := os.Open("Covid-19_std_small.csv")
    if err != nil {
        fmt.Println(err)
    }

    defer csvFile.Close()
    
    csvLines, err := csv.NewReader(csvFile).ReadAll()
    if err != nil {
        fmt.Println(err)
    }

    var first string

    fmt.Scanln(&first)

    switch first{
    case "SELECT":
        fmt.Println("SE HA SELECCIONADO FUNCION SELECT")
        fmt.Println(csvLines[0])


        //USER INPUT
        in := bufio.NewReader(os.Stdin)
        linee, _ := in.ReadString('\n')
        col_name := strings.Replace(linee,"\n","",-1)
        //SE ASUME QUE SE INGRESAN NOMBRES DE COLUMNAS

        var filter string
        fmt.Scanln(&filter)

        in2 := bufio.NewReader(os.Stdin)
        line2, _ := in2.ReadString('\n')
        value := strings.Replace(line2,"\n","",-1)

        //SIGUIENDO TUTORIAL MAP REDUCE
        lists := make(chan []M)
        finalValue := make(chan []M)

        var wg sync.WaitGroup

        wg.Add(len(csvLines))

        //MAP
        for _, line:= range csvLines{
            go func(dataa []string){
                defer wg.Done()
                lists <- MapSelect(dataa)
            }(line)
        }

        //REDUCE
        go ReduceSelect(col_name, filter, value, lists, finalValue)

        wg.Wait()
        close(lists)

        mapToCSV(<- finalValue, first)

    case "PROJECTION":
        fmt.Println("SE HA SELECCIONADO FUNCION PROJECTION")
        
        //INPUTS USUARIO
        var number_of_columns int
        fmt.Scanln(&number_of_columns)

        //LISTA DE LAS KEYS QUE NOS INTERESAN
        var keysToUse []string

        for i := 0; i < number_of_columns; i++ {
            
            in := bufio.NewReader(os.Stdin)
            linee, _ := in.ReadString('\n')
            lineee := strings.Replace(linee,"\n","",-1)

            //SE ASUME QUE SE INGRESAN NOMBRES DE COLUMNAS
            keysToUse = append(keysToUse, lineee)

            
        }

        fmt.Println(keysToUse)
        
        //SIGUIENDO TUTORIAL MAP REDUCE
        lists := make(chan []M)
        finalValue := make(chan []M)

        var wg sync.WaitGroup

        wg.Add(len(csvLines))

        //MAP
        for _, line:= range csvLines{
            go func(dataa []string){
                defer wg.Done()
                lists <- MapProjection(keysToUse, dataa)
            }(line)
        }

        //REDUCE
        go ReduceProjection(lists, finalValue)

        wg.Wait()
        close(lists)

        mapToCSV(<- finalValue, first)

    case "GROUP":
        /*
        in2 := bufio.NewReader(os.Stdin)
        line2, _ := in2.ReadString('\n')
        col_name0 := strings.Replace(line2,"\n","",-1)

        var aggregate string
        fmt.Scanln(&aggregate)

        in3 := bufio.NewReader(os.Stdin)
        line3, _ := in3.ReadString('\n')
        col_name1 := strings.Replace(line3,"\n","",-1)

        var functionSelect string

        fmt.Scanln(&functionSelect)

        //SIEMPRE AGREGA, pedir input AGGREGATE es una formalidad
        groupAggregateFunction(col_name0, col_name1, functionSelect)
        */
    default:
        fmt.Println("NO ES OPERACION VALIDA")
    
    }

}


