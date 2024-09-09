COPY (SELECT * FROM (
    SELECT image.id AS imageId,
           pixels.id AS pixelsId,
           image.series,
           pixelstype.value AS pixelstype,
           pixels.sizeX,
           pixels.sizeY,
           pixels.sizeZ,
           pixels.sizeC,
           pixels.sizeT,
           format.value,
           rank() OVER (PARTITION BY fileset.id ORDER BY image.id) AS rank
        FROM fileset
            JOIN image ON fileset.id = image.fileset
            JOIN pixels ON image.id = pixels.image
            JOIN pixelstype ON pixels.pixelstype = pixelstype.id
            JOIN format ON image.format = format.id
) AS rank WHERE rank.rank = 1) TO STDOUT CSV;
