-- perguntas:
-- 1. qual funcionario que mais atende requisicoes no suporte?
-- 2. quais sao os funcionarios que sao chefes?
-- 3. quais sao os chefes com o maior numero de funcinarios abaixo?
-- 4. para cada estilo musical, top3 com as músicas mais longas e o nome dos artistas
-- 5. qual o artista com o maior numero de faixas?
-- 6. qual a faixa que aparece em mais de uma playlist?

-- 1
select E.EmployeeId, E.FirstName || ' ' || E.LastName full_name, count(C.SupportRepId) replies
from Employee E
    left join Customer C on E.EmployeeId = C.SupportRepId
group by E.EmployeeId, E.FirstName || ' ' || E.LastName
order by replies desc
;

select E.EmployeeId, E.FirstName || ' ' || E.LastName full_name, count(C.SupportRepId) replies
from Employee E
    inner join Customer C on E.EmployeeId = C.SupportRepId
group by E.EmployeeId, E.FirstName || ' ' || E.LastName
order by replies desc
;

select C.SupportRepId, count(C.SupportRepId) replies
from Customer C
group by C.SupportRepId
order by replies desc
;

-- 2
select *
from Employee
where ReportsTo is null
;

select distinct Title
from Employee
;

select *
from Employee
where Title = 'General Manager'
;

select *
from Employee
where Title like '%Manager%'
;

-- 3
select E1.ReportsTo, E2.FirstName || ' ' || E2.LastName full_name, count(E1.ReportsTo) employees
from Employee E1
    inner join Employee E2 on E1.ReportsTo = E2.EmployeeId
group by E1.ReportsTo, E2.FirstName || ' ' || E2.LastName
order by employees desc
;

-- 4
select *
from (
    select T.GenreId, G.Name, T.Name, T.Milliseconds,
           row_number() over win as idx
    from Track T
        inner join Genre G on T.GenreId = G.GenreId
    window win as (partition by T.GenreId order by T.Milliseconds desc)
    order by T.GenreId
) tmp
where idx in (1, 2, 3)
;

select tmp.*, AR.Name artist_name
from (
    select T.GenreId, G.Name, T.Name, T.Milliseconds, T.AlbumId,
           row_number() over win as idx
    from Track T
        inner join Genre G on T.GenreId = G.GenreId
    window win as (partition by T.GenreId order by T.Milliseconds desc)
    order by T.GenreId
) tmp
inner join Album A on tmp.AlbumId = A.AlbumId
inner join Artist AR on A.ArtistId = AR.ArtistId
where tmp.idx in (1, 2, 3)
;

-- 5
select A.Name, count(T.TrackId) tracks
from Artist A
inner join Album AL on A.ArtistId = AL.ArtistId
inner join Track T on AL.AlbumId = T.AlbumId
group by A.Name
order by tracks desc
;

-- 6
select T.TrackId, count(P.PlaylistId) playlists
from Track T
inner join PlaylistTrack PT on T.TrackId = PT.TrackId
inner join Playlist P on PT.PlaylistId = P.PlaylistId
group by T.TrackId
order by playlists desc
;

select A2.ArtistId, A2.Name, count(P.PlaylistId) playlists
from Track T
inner join PlaylistTrack PT on T.TrackId = PT.TrackId
inner join Playlist P on PT.PlaylistId = P.PlaylistId
inner join Album A on T.AlbumId = A.AlbumId
inner join Artist A2 on A.ArtistId = A2.ArtistId
inner join Genre G on T.GenreId = G.GenreId
group by A2.ArtistId, A2.Name
order by playlists desc
;

-- Extra
select date(InvoiceDate) invoice_dt, sum(Total)
from Invoice I
inner join InvoiceLine IL on I.InvoiceId = IL.InvoiceId
group by date(InvoiceDate)
order by invoice_dt
;

select InvoiceLineId, UnitPrice, Quantity, (UnitPrice * Quantity) total_amount
from InvoiceLine
;

select tmp.Name,
       tmp.tracks_sold,
       tmp.tracks_on_playlist,
       round(tracks_sold / (tracks_on_playlist * 1.00), 3) avg_tracks
from (
    select A.Name,
           (select count(InvoiceLineId)
            from InvoiceLine
                inner join Track T on InvoiceLine.TrackId = T.TrackId
                inner join Album A2 on T.AlbumId = A2.AlbumId
            where A2.ArtistId = A.ArtistId
           ) tracks_sold,
           (select count(0)
            from PlaylistTrack
               inner join Track T2 on PlaylistTrack.TrackId = T2.TrackId
               inner join Album A3 on T2.AlbumId = A3.AlbumId
            where A3.ArtistId = A.ArtistId
           ) tracks_on_playlist
    from Artist A
) tmp
where tmp.tracks_sold > 9
order by avg_tracks desc
;
