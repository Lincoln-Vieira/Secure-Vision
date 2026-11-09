async function realizarLogout(event) {
    event.preventDefault();

    try {
        const respostaCsrf = await fetch("/csrf");
        const csrf = await respostaCsrf.json();

        await fetch("/logout", {
            method: "POST",
            headers: {
                [csrf.headerName]: csrf.token
            }
        });

        window.location.href = "/login.html?logout=true";
    } catch (erro) {
        alert("Não foi possível sair. Tente novamente.");
        console.error(erro);
    }

    return false;
}
