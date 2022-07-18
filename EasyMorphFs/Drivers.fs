namespace EasyMorphFs

open Microsoft.FSharp.Core
open Data
open System
open System.Threading
open System.Threading.Tasks
open System.IO

module Drivers =

    type SymbolType = 
    | Nothing = 0uy
    | Int8 = 1uy
    | Int16 = 2uy
    | Int32 = 4uy
    | Decimal = 16uy
    | Text = 32uy
    | BoolTrue = 64uy
    | BoolFalse = 65uy
    | Error = 128uy

    type ImportConfig = {
        FileName : string
        Token : CancellationToken
    }
    with 
        static member Make fileName = { FileName = fileName; Token = CancellationToken.None }

    type IImportDriver = 
        abstract member ImportAsync : config:ImportConfig -> Task<IDataset[]>

    type ImportDriver () as me =

        //Version of dset file
        let VERSION00 = "HIBC00"
        let VERSION01 = "HIBC01"
        //Constant which defines compressed type of column in file
        let COMPRESSED_COLUMN_TYPE_NAME = "Column.Compressed"
        //Constant which defines constant type of column in file
        let CONSTANT_COLUMN_TYPE_NAME = "Column.Constant"
        //Item types
        let FIELD_ITEM_TYPE = "EasyMorph.CompressedField.0"
        let TABLE_METADATA_ITEM_TYPE = "EasyMorph.TableMetadata.0"
        let COLUMNS_METADATA_ITEM_TYPE = "EasyMorph.ColumnsMetadata.0"


        let readCell (br:BinaryReader) =
            match br.ReadByte () |> LanguagePrimitives.EnumOfValue with
            | SymbolType.Nothing -> CellType.Nothing, box ()
            | SymbolType.Int8 -> CellType.Number, box (decimal (br.ReadSByte ()))
            | SymbolType.Int16 -> CellType.Number, box (decimal (br.ReadInt16 ()))
            | SymbolType.Int32 -> CellType.Number, box (decimal (br.ReadInt32 ()))
            | SymbolType.Decimal -> CellType.Number, box (br.ReadDecimal ())
            | SymbolType.Text -> CellType.Text, box (br.ReadString ())
            | SymbolType.BoolTrue -> CellType.Boolean, box (true)
            | SymbolType.BoolFalse -> CellType.Boolean, box (false)
            | SymbolType.Error -> 
                br.ReadInt32 () |> ignore    //Error code. Not used as of now. Reserved for future versions.
                CellType.Error, box (br.ReadString ())
            | x ->
                raise (Exception(sprintf "Unknown symbol type: %A" x))

        let readCompressedColumn (br:BinaryReader) fieldName (token:CancellationToken) = 
            async {
                token.ThrowIfCancellationRequested ()

                let vocabularyLength = br.ReadInt32 ()
                return 0
            }

        interface IImportDriver with
            member this.ImportAsync config = 
                async {
                    // Validate input
                    if String.IsNullOrWhiteSpace config.FileName then
                        raise (ArgumentException ("Should be not empty", nameof(config.FileName)))
                
                    if File.Exists config.FileName |> not then
                        raise (FileNotFoundException("File not found.", config.FileName))

                    // Non-idiomatic for F#. Need to rethink.
                    let columns = System.Collections.Generic.List<IColumn>()
                    let mutable tableName = ""

                    use fs = new FileStream(config.FileName, FileMode.Open, FileAccess.Read, FileShare.Read)
                    use br = new BinaryReader(fs)

                    //Read version from file
                    let version = string(br.ReadChars(6));

                    if not (version.Equals(VERSION00)) && not (version.Equals(VERSION01)) then
                        raise (Exception("Wrong file header format."))

                    while fs.Position < fs.Length do 
                        config.Token.ThrowIfCancellationRequested ()

                    // Item name
                    tableName <- br.ReadString ()
                    // Item type
                    let itemType = br.ReadString ()

                    //Length of current block(field)
                    let blockLength = if version.Equals(VERSION00) then int64 (br.ReadInt32 ()) else br.ReadInt64 ();
                    let blockStartPosition = fs.Position;

                    // Only non-encrypted field items are supported
                    if itemType = FIELD_ITEM_TYPE then
                        ignore()
                        //let! column = ReadColumn(br, config.Token);

                        //Add columns to result
                        //columns.Add(column);

                    // Metadata items are ignored
                    elif (itemType <> TABLE_METADATA_ITEM_TYPE) && (itemType <> COLUMNS_METADATA_ITEM_TYPE) then
                        raise (Exception(sprintf "Unsupported item type: %A" itemType));

                    //Seek to end of block
                    fs.Seek(blockStartPosition + blockLength, SeekOrigin.Begin) |> ignore;

                    return Array.empty<IDataset>
                } |> Async.StartAsTask




