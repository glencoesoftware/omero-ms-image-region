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
           e2.time AS importTime,
           e2.time - e1.time AS setId,
           rank() OVER (PARTITION BY fileset.id ORDER BY image.id) AS rank
        FROM fileset
            JOIN image ON fileset.id = image.fileset
            JOIN pixels ON image.id = pixels.image
            JOIN pixelstype ON pixels.pixelstype = pixelstype.id
            JOIN format ON image.format = format.id
            JOIN event e2 on image.creation_id=e2.id
            JOIN filesetjoblink on  filesetjoblink.parent=fileset.id
            JOIN job on filesetjoblink.child=job.id
            JOIN uploadjob on job.id=uploadjob.job_id
            JOIN event e1 on job.update_id=e1.id
)  AS query WHERE query.rank = 1 AND query.importTime > :start_date ORDER BY query.setId desc) TO STDOUT CSV;
