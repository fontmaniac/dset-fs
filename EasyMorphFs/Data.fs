namespace EasyMorphFs

open Microsoft.FSharp.Core

module Data =

    type CellType = 
    | Number = 0
    | Text = 1
    | Boolean = 2
    | Nothing = 3
    | Error = 4

    type Nothing = Unit

    type VocabularyItem = {
        Type : CellType
        Index : int
    }

    type IAccess =
        abstract member GetError : i:int -> option<string>
        abstract member GetText : i:int -> option<string>
        abstract member GetNumber : i:int -> option<decimal>
        abstract member GetBoolean : i:int -> option<bool>
        abstract member GetNothing : i:int -> option<Nothing>
        abstract member GetType : i:int -> CellType

    type IColumn = 
        inherit IAccess
        abstract member Name : string with get
        abstract member Length : int with get

    type IDataset = 
        abstract member Name : string with get
        abstract member Columns : IColumn[] with get
        abstract member ColumnsCount : int with get
        abstract member RowsCount : int with get
        abstract member GetRow : rowIndex:int -> IAccess
    
    type Vocabulary (strings : string[], decimals : decimal[], items : VocabularyItem[]) = 
        let get i cellType make = 
            let item = items.[i]
            if item.Type = cellType then Some(make item.Index) else None

        interface IAccess with
            member this.GetError i      = get i CellType.Error   (fun idx -> strings.[idx])
            member this.GetText i       = get i CellType.Text    (fun idx -> strings.[idx])
            member this.GetNumber i     = get i CellType.Number  (fun idx -> decimals.[idx])
            member this.GetBoolean i    = get i CellType.Boolean (fun idx -> idx = 1)
            member this.GetNothing i    = get i CellType.Nothing (fun _ -> ())
            member this.GetType i       = items.[i].Type

    type ConstantColumn (length, constant, type', name) = 
        let get cellType make = 
            if type' = cellType then Some(make ()) else None

        interface IColumn with
            member this.Name = name
            member this.Length = length
            member this.GetError i      = get CellType.Error   (fun () -> unbox<string> constant)
            member this.GetText i       = get CellType.Text    (fun () -> unbox<string> constant)
            member this.GetNumber i     = get CellType.Number  (fun () -> unbox<decimal> constant)
            member this.GetBoolean i    = get CellType.Boolean (fun () -> unbox<bool> constant)
            member this.GetNothing i    = get CellType.Nothing (fun () -> unbox<Nothing> constant)
            member this.GetType i       = type'

    type CompressedColumn (vectorBytes:byte[], vocabulary:IAccess, bitWidth, length, name) = 
        let _mask = (1UL <<< bitWidth) - 1UL;
        let getVocabularyIndex rowIndex =
            //calculate firstBut position in byte array
            let firstBit = (uint64 rowIndex) * (uint64 bitWidth)
            //byte index where first bit is located
            let byteIndex = int (firstBit / 8UL)
            //Bit number in first byte
            let byteOffset = int (firstBit % 8UL)
            //Move first bit to 0 position and store to result
            let result = uint64 (vectorBytes.[int byteIndex] >>> byteOffset)
            //calculate number of read bits
            let bitsRead = 8 - byteOffset;

            let rec read byteIndex bitsRead result =
                //while need to read more bits
                if bitsRead >= bitWidth then result
                else
                    //increase byte index
                    let byteIndex = byteIndex + 1;
                    //store new byte in result
                    let result = uint64 (vectorBytes.[byteIndex]) <<< bitsRead ||| result
                    read byteIndex (bitsRead + 8) result

            //calc result and trim with bit mask
            read byteIndex bitsRead result 
            |> fun x -> x &&& _mask
            |> int
            
        interface IColumn with
            member this.Name = name
            member this.Length = length
            member this.GetError i      = i |> getVocabularyIndex |> vocabulary.GetError
            member this.GetText i       = i |> getVocabularyIndex |> vocabulary.GetText
            member this.GetNumber i     = i |> getVocabularyIndex |> vocabulary.GetNumber
            member this.GetBoolean i    = i |> getVocabularyIndex |> vocabulary.GetBoolean
            member this.GetNothing i    = i |> getVocabularyIndex |> vocabulary.GetNothing
            member this.GetType i       = i |> getVocabularyIndex |> vocabulary.GetType

    type Dataset (columns, name) = 
        interface IDataset with
            member this.Name = name
            member this.Columns = columns
            member this.ColumnsCount = columns.Length
            member this.RowsCount = if columns.Length > 0 then columns.[0].Length else 0
            member this.GetRow rowIndex =
                { new IAccess with 
                    member _.GetError i      = columns.[i].GetError(rowIndex)
                    member _.GetText i       = columns.[i].GetText(rowIndex)
                    member _.GetNumber i     = columns.[i].GetNumber(rowIndex)
                    member _.GetBoolean i    = columns.[i].GetBoolean(rowIndex)
                    member _.GetNothing i    = columns.[i].GetNothing(rowIndex)
                    member _.GetType i       = columns.[i].GetType(rowIndex)
                }





        