package main

import (
    //"bufio"   // Leer STDIN
    //"flag"    // Leer flags
    "fmt"     // Print
    //"log"     // Escribir Logs
    "os"      // Leer sistema de archivos
    //"runtime" // Imprimir # de GoRouitnes
    //"strings" // Usar Replace()
    "sync"
    "encoding/csv"     // Leer archivos csv

    //"strconv" // Convertir de string a int
)

// filename = Covid-19_std.csv

type M map[string]string

type structData struct {

    Region string
    Codigo_Region string
    Comuna string
    Codigo_Comuna string
    Poblacion string
    Fecha string
    Casos string
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

func MapProjection(line []string)[]M{

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



func ReduceProjection(keysToUse []string, mapList chan[]M, sendFinalValue chan[]M){

    var final []M

    for list:= range mapList{
            for _, value := range list{

                req := M{}
               

                for _, neededKey := range keysToUse{
                    req[neededKey] = value[neededKey]
                }

                final = append(final, req)
            }
        }
    sendFinalValue <- final
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
        var col_name string
        fmt.Scanln(&col_name)

        var filter string
        fmt.Scanln(&filter)

        var value string
        fmt.Scanln(&value)

        //


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

        fmt.Println(<- finalValue)

    case "PROJECTION":
        fmt.Println("SE HA SELECCIONADO FUNCION PROJECTION")
        
        //INPUTS USUARIO
        var number_of_columns int
        fmt.Scanln(&number_of_columns)

        //LISTA DE LAS KEYS QUE NOS INTERESAN
        var keysToUse []string

        for i := 0; i < number_of_columns; i++ {
            var keyToAdd string
            fmt.Scanln(&keyToAdd)

            //SE ASUME QUE SE INGRESAN NOMBRES DE COLUMNAS
            keysToUse = append(keysToUse, keyToAdd)
        }

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
        go ReduceProjection(keysToUse, lists, finalValue)

        wg.Wait()
        close(lists)

        fmt.Println(<- finalValue)

    case "GROUP":
        var col_name0 string
        fmt.Scanln(&col_name0)

        var aggregate string
        fmt.Scanln(&aggregate)

        var col_name1 string
        fmt.Scanln(&col_name1)

        var functionSelect string

        fmt.Scanln(&functionSelect)

        //SIEMPRE AGREGA, pedir input AGGREGATE es una formalidad
        //selectFunction(col_name0, col_name1, functionSelect)
    default:
        fmt.Println("NO ES OPERACION VALIDA")
    
    }

    /*
    for _, line := range csvLines {
        data := empData{
            Region: line[0],
            Codigo_Region : line[1],
            Comuna : line[2],
            Codigo_Comuna : line[3],
            Poblacion : line[4],
            Fecha : line[5],
            Casos : line[6],
        }
        fmt.Println(data.Region + " " + data.Codigo_Comuna + " " + data.Casos)
    }
    */
}

// Hacer lo mismo que en la ayudantía para el select

