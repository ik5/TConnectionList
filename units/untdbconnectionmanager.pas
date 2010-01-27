{
  Database connection manager for Lazarus/FPC

  Manages multiple requests for the same queries from different sources and
  manage the opening and closing of each query request.

  Copyright (C) 2010 LINESIP idok at@at linesip dot.dot com

  This library is free software; you can redistribute it and/or modify it
  under the terms of the GNU Library General Public License as published by
  the Free Software Foundation; either version 2 of the License, or (at your
  option) any later version.

  This program is distributed in the hope that it will be useful, but WITHOUT
  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
  FITNESS FOR A PARTICULAR PURPOSE. See the GNU Library General Public License
  for more details.

  You should have received a copy of the GNU Library General Public License
  along with this library; if not, write to the Free Software Foundation,
  Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
}
(*
History:
  18-22/01/2010 - Initial implementation - quick and dirty implementation
                  required to be rewriten for feature use.
*)

{ TODO : Replace current implementation into TCollection }
{ TODO : Add support for ltDynamic }

unit untDBConnectionManager;

{$mode objfpc}

interface

uses Classes, SysUtils, db;

const
  VERSION = '0.1';

type
  EConnectionManager   = class(Exception);
  EDuplicateConnection = class(EConnectionManager);
  EConnectionOpen      = class(EConnectionManager);
  EConnectionClose     = class(EConnectionManager);

  { The type of TConnectionList }
  TListType = (
               {
                 The first owner that opened the connection is the one that
                 will actually close the connection
               }
               ltOpenClose,

               {
                 The list count each connection request for each query and the
                 first request will open a connection, and the last request to
                 close it, will close the connection itself
               }
               ltNumerical,

               {
                 The list creates the connection on it's own on demand and also
                 free it on demand/when TConnectionList is freed.

                 IMPORTANT: At the moment (0.1) this feature is not supported,
                            and there is not code to handle it but the settings
                            exists for feature versions !
               }
               ltDynamic
              );


  { A pointer to the connection information record }
  PConnectionItem = ^TConnectionItem;
  { A record that save a connection information }
  TConnectionItem = packed record
    { The actual connection }
    Connection  : TDBDataset;
    {
      Who made calls for this connection.
      It stores also information about how many requests where made,
      and item #0 is the first caller
    }
    Callers     : TList;
    {
      Allow us to know if we are the one to free the connection or not.
      This feature is corrently just for design but without any code for it.
    }
    DynCreate   : Boolean;
  end;

  { TConnectionList }

  TConnectionList = class(TPersistent)
  private
    FListType : TListType;
    FList     : TList;
  public
    constructor Create( Const aListType : TListType = ltOpenClose);
    destructor Destroy; override;

    function Add(aConnection : TDBDataset) : integer; virtual;
    procedure LocateItem(aConnection : TDBDataset;
                         out index : Integer; Rec : PConnectionItem); virtual;
    procedure LocateItem(aConnection : TDBDataset;
                         out index : Integer; out Rec : TConnectionItem); virtual;
    function IndexOf(aConnection : TDBDataset) : Integer; virtual;
    function Find(aConnection : TDBDataset) : TConnectionItem; virtual;

    procedure OpenConnection(aConnection : TDBDataset; Caller : TObject); virtual;
    procedure CloseConnection(aConnection : TDBDataset; Caller : TObject); virtual;

    property ListType : TListType read FListType;
  end;

resourcestring
  sErrDynamicNotSupported   = 'The dynamic option is not yet supported'        ;
  sErrAlreadyExists         = '%s already exists on the list'                  ;
  sErrConnectionAlreadyOpen = '%s is already open outside the connection list.'+
                              '%sThe list can not control this connection'     ;
  sErrConnectionNotFound    = '%s was not found on the list'                   ;
  sErrClassOpenerNotFound   = '%s never made an open request'                  ;
  sErrClassIsOpenClass      = '%s is not the last class on the list, even '    +
                              'though it opened the connection'                ;


implementation

{ TConnectionList }

procedure TConnectionList.LocateItem ( aConnection : TDBDataset; out
  index : Integer; out Rec : TConnectionItem ) ;

begin
  LocateItem(aConnection, index, @Rec);
end;

procedure TConnectionList.LocateItem ( aConnection : TDBDataset; out
  index : Integer; Rec : PConnectionItem ) ;
var
  i : integer;
begin
  index := -1;
  FillChar(Rec,sizeof(TConnectionItem),0);
  for i := 0 to FList.Count -1 do
    begin
      if TConnectionItem(FList.Items[i]^).Connection = aConnection then
        begin
          Index := i;
          Rec   := PConnectionItem(FList.Items[i]);
          break;
        end;
    end;
end;

function TConnectionList.Find ( AConnection : TDBDataset ) : TConnectionItem;
var
  tmp : Integer;
begin
  LocateItem(aConnection, tmp, Result);
end;

function TConnectionList.IndexOf ( aConnection : TDBDataset ) : Integer;
var
  tmp : TConnectionItem;
begin
  LocateItem(aConnection, Result, tmp);
end;

constructor TConnectionList.Create ( const aListType : TListType );
begin
  if aListType = ltDynamic then
    raise Exception.Create(sErrDynamicNotSupported);

  FListType := aListType;
  FList     := TList.Create;
end;

destructor TConnectionList.Destroy;
begin
  FreeAndNil(FList); // To check if it creates a memory leak
  inherited Destroy;
end;

function TConnectionList.Add ( aConnection : TDBDataset ) : integer;
var
  Item : TConnectionItem;
begin
  FillChar(Item, sizeof(TConnectionItem), 0);
  Result := self.IndexOf(aConnection);

  if Result <> -1 then // Not efficient -> need to find a better way to do it
    begin
      raise EConnectionManager.CreateFmt(sErrAlreadyExists, [aConnection.Name]);
    end;
  Item.Connection := aConnection; // copying a connection address

  Result := FList.Add(@Item);
end;

procedure TConnectionList.OpenConnection ( aConnection : TDBDataset;
  Caller : TObject ) ;
var
  Item  : TConnectionItem;
  pItem : ^TConnectionItem;
  i     : integer;

begin
  FillChar(Item,  SizeOf(TConnectionItem), 0);
  LocateItem(aConnection, i, pItem);
  if i <> -1 then // Found the connection
    begin
      // do we have the caller already
      if pItem^.Callers.IndexOf(Caller) <> -1 then
      begin
        raise EConnectionManager.CreateFmt(sErrAlreadyExists,
              [Caller.ClassName]);
      end;

      pItem^.Callers.Add(Caller);
    end
  else begin // the connection was not found
    if aConnection.Active then
      begin
        raise EConnectionOpen.CreateFmt(sErrConnectionAlreadyOpen,
                                        [aConnection.Name, LineEnding]);
      end;
    aConnection.Open; // Open the actual connection, because no one had opened
                      // it so far
    Item.Connection := aConnection;
    Item.DynCreate  := False;
    if not Assigned(Item.Callers) then // if we do not have initialized list
      begin                            // then initialize it
        Item.Callers := TList.Create;
      end;

    Item.Callers.Add(Caller);

    FList.Add(@Item);
  end;

end;

procedure TConnectionList.CloseConnection ( aConnection : TDBDataset;
  Caller : TObject ) ;
var
  Item      : PConnectionItem;
  index     : integer;
  CanClose  : Boolean;
  CallerPos : Integer;

  procedure RemoveLast; inline;
  begin
    {
      It's the only item on the list, it's safe to fully dissconnect and remove
      from the list.
    }
    if (Item^.Callers.Count = 1) then
      begin
        if Item^.Connection.Active then
          begin
            Item^.Connection.Close;
          end;
        FreeAndNil(Item^.Callers);
        FList.Delete(Index);
        Freemem(Item);
        Item := nil;
    end; // if (Item^.Callers.Count = 1)
  end; // procedure RemoveLast; inline;

  procedure RemoveOthers; inline;
  begin
    if Item^.Callers.Count > 1 then
      begin
        Item^.Callers.Delete(CallerPos);
      end; // if Item^.Callers.Count > 1
  end; // procedure RemoveOthers; inline;

  procedure ItemNotFound(const Name : String); inline;
  begin
   raise EConnectionClose.CreateFmt(sErrConnectionNotFound, [Name]);
  end; // procedure ItemNotFound(const Name : String); inline;

begin
  CanClose := False;
  LocateItem(aConnection, index, Item);

  if index = -1 then // Can not close something that is not existed
    begin
      ItemNotFound(aConnection.Name);
    end;

  CallerPos := Item^.Callers.IndexOf(Caller);

  case FListType of
    ltOpenClose : begin
                    case CallerPos of
                      -1 : begin // Not Found
                             ItemNotFound(Caller.ClassName);
                           end;

                      0  : begin // The first location on the list
                           {
                             More items on the list so we can not dissconnect
                             the connection with the first caller, so raising
                             an exception that the developer must first close
                             other connections and only then s/he can call it
                             to be closed. That's what ltOpenClose is all about
                           }
                             if (Item^.Callers.Count > 1) then
                               begin
                                 raise EConnectionClose.CreateFmt(
                                      sErrClassIsOpenClass, [Caller.ClassName]);
                               end
                             else begin
                               RemoveLast;
                             end; // else begin
                           end; // 0  : begin
                      else // Other location on the list
                        begin
                          RemoveOthers;
                        end; // else begin

                    end; // case CallerPos of
                  end; // ltOpenClose : begin

    ltNumerical : begin
                    case CallerPos of
                      -1 : begin
                             ItemNotFound(Caller.ClassName);
                           end; // -1 : begin
                       0 : begin
                             RemoveLast;   // Try to free the last item
                             RemoveOthers; // if it was not the last item, free
                                           // it as a normal item
                           end; // 0 : begin
                      else begin
                             RemoveOthers;
                           end; // else begin
                    end; // case CallerPos of
                  end; // ltNumerical : begin

    ltDynamic   : ;
  end;
end;

end.

