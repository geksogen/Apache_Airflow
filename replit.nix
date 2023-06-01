{ pkgs }: {
  deps = [
    pkgs.openssh_with_kerberos
    pkgs.sudo
    pkgs.openssh_with_kerberos
    pkgs.openssh_with_kerberos
    pkgs.sudo
    pkgs.openssh_with_kerberos
    pkgs.openssh_with_kerberos
    pkgs.import requests
    pkgs.openssh_with_kerberos
    pkgs.sudo
    pkgs.openssh_with_kerberos
    pkgs.bashInteractive
    pkgs.nodePackages.bash-language-server
    pkgs.man
  ];
}