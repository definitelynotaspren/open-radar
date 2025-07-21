async function ingestData(evt) {
    evt.preventDefault();
    const form = document.getElementById('dataForm');
    const files = document.getElementById('fileUpload').files;
    const urls = document.getElementById('urlInput').value;
    const flagReason = document.getElementById('flagReason').value;
    const apiKey = document.getElementById('apiKey').value;

    const formData = new FormData();
    for (const f of files) {
        formData.append('files', f);
    }
    formData.append('urls', urls);
    formData.append('api_key', apiKey);
    formData.append('flag_reason', flagReason);
    // placeholder user id; in real setup this would come from auth
    formData.append('user', 'definitelynotaspren');

    const status = document.getElementById('status');
    status.textContent = 'Ingesting...';
    try {
        const resp = await fetch('/ingest', { method: 'POST', body: formData });
        const data = await resp.json();
        status.textContent = 'Ingest complete';
        if (data.audit) {
            document.getElementById('auditLogSection').style.display = 'block';
            const logText = await (await fetch('/audit-log?user=definitelynotaspren')).text();
            document.getElementById('auditLog').textContent = logText;
        }
    } catch (err) {
        status.textContent = 'Error running ingest';
    }
}

document.getElementById('dataForm').addEventListener('submit', ingestData);

document.getElementById('downloadPublic').addEventListener('click', () => {
    window.location = '/download/public';
});

document.getElementById('downloadPrivate').addEventListener('click', () => {
    window.location = '/download/private?user=definitelynotaspren';
});
